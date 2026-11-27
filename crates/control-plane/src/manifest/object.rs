use k8s_openapi::{
    api::{
        apps::v1::{Deployment, StatefulSet},
        core::v1::{PersistentVolume, PersistentVolumeClaim, Secret, Service},
    },
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
    Resource,
};
use serde::de::Error as _;
use serde_json::Value;

use crate::{ids::Generation, materialization::RenderedObjectRef};

#[derive(Clone, Debug, PartialEq)]
pub struct RenderedManifest {
    pub instance_generation: Generation,
    pub objects: Vec<RenderedManifestObject>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RenderedManifestObject {
    pub apply_order: ApplyOrder,
    pub object: KubernetesObject,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ApplyOrder {
    PersistentVolume,
    PersistentVolumeClaim,
    Secret,
    Service,
    Workload,
}

/// One rendered object of a kind the control plane applies, held as its
/// `k8s-openapi` type.
#[derive(Clone, Debug, PartialEq)]
pub struct KubernetesObject(Body);

#[derive(Clone, Debug, PartialEq)]
enum Body {
    Deployment(Box<Deployment>),
    StatefulSet(Box<StatefulSet>),
    Service(Box<Service>),
    Secret(Box<Secret>),
    PersistentVolume(Box<PersistentVolume>),
    PersistentVolumeClaim(Box<PersistentVolumeClaim>),
}

/// Evaluates `$expr` with `$object` bound to the typed object, whatever its kind.
macro_rules! each_kind {
    ($body:expr, $object:ident => $expr:expr) => {
        match $body {
            Body::Deployment($object) => $expr,
            Body::StatefulSet($object) => $expr,
            Body::Service($object) => $expr,
            Body::Secret($object) => $expr,
            Body::PersistentVolume($object) => $expr,
            Body::PersistentVolumeClaim($object) => $expr,
        }
    };
}

macro_rules! from_kind {
    ($($kind:ident),*) => {$(
        impl From<$kind> for KubernetesObject {
            fn from(object: $kind) -> Self {
                Self(Body::$kind(Box::new(object)))
            }
        }
    )*};
}

from_kind!(
    Deployment,
    StatefulSet,
    Service,
    Secret,
    PersistentVolume,
    PersistentVolumeClaim
);

impl KubernetesObject {
    /// Reads an object of a supported kind from its Kubernetes JSON. Fields
    /// outside the kind's schema are dropped.
    pub fn from_kubernetes_json(value: Value) -> Result<Self, serde_json::Error> {
        fn typed<K>(value: Value) -> Result<Body, serde_json::Error>
        where
            K: serde::de::DeserializeOwned,
            KubernetesObject: From<K>,
        {
            serde_json::from_value::<K>(value).map(|object| KubernetesObject::from(object).0)
        }
        let type_meta = (
            value["apiVersion"].as_str().unwrap_or_default().to_owned(),
            value["kind"].as_str().unwrap_or_default().to_owned(),
        );
        let body = match (type_meta.0.as_str(), type_meta.1.as_str()) {
            (Deployment::API_VERSION, Deployment::KIND) => typed::<Deployment>(value),
            (StatefulSet::API_VERSION, StatefulSet::KIND) => typed::<StatefulSet>(value),
            (Service::API_VERSION, Service::KIND) => typed::<Service>(value),
            (Secret::API_VERSION, Secret::KIND) => typed::<Secret>(value),
            (PersistentVolume::API_VERSION, PersistentVolume::KIND) => {
                typed::<PersistentVolume>(value)
            }
            (PersistentVolumeClaim::API_VERSION, PersistentVolumeClaim::KIND) => {
                typed::<PersistentVolumeClaim>(value)
            }
            (api_version, kind) => Err(serde_json::Error::custom(format!(
                "{api_version} {kind} is not a kind the control plane applies"
            ))),
        }?;
        Ok(Self(body))
    }

    pub fn api_version(&self) -> &'static str {
        each_kind!(&self.0, object => api_version_of(object.as_ref()))
    }

    pub fn kind(&self) -> &'static str {
        each_kind!(&self.0, object => kind_of(object.as_ref()))
    }

    pub fn name(&self) -> &str {
        self.metadata().name.as_deref().unwrap_or_default()
    }

    /// Empty for a cluster-scoped object.
    pub fn namespace(&self) -> &str {
        self.metadata().namespace.as_deref().unwrap_or_default()
    }

    pub fn object_ref(&self) -> RenderedObjectRef {
        RenderedObjectRef {
            api_version: self.api_version().to_owned(),
            kind: self.kind().to_owned(),
            namespace: self.namespace().to_owned(),
            name: self.name().to_owned(),
        }
    }

    pub fn metadata(&self) -> &ObjectMeta {
        each_kind!(&self.0, object => &object.metadata)
    }

    pub fn metadata_mut(&mut self) -> &mut ObjectMeta {
        each_kind!(&mut self.0, object => &mut object.metadata)
    }

    /// The metadata of the Pods a workload controller creates.
    pub fn pod_template_metadata(&self) -> Option<&ObjectMeta> {
        match &self.0 {
            Body::Deployment(object) => object.spec.as_ref()?.template.metadata.as_ref(),
            Body::StatefulSet(object) => object.spec.as_ref()?.template.metadata.as_ref(),
            _ => None,
        }
    }

    pub fn pod_template_metadata_mut(&mut self) -> Option<&mut ObjectMeta> {
        let template = match &mut self.0 {
            Body::Deployment(object) => &mut object.spec.as_mut()?.template,
            Body::StatefulSet(object) => &mut object.spec.as_mut()?.template,
            _ => return None,
        };
        Some(template.metadata.get_or_insert_with(ObjectMeta::default))
    }

    /// The body the control plane sends to the API server.
    pub fn to_kubernetes_json(&self) -> Value {
        each_kind!(&self.0, object => serde_json::to_value(object.as_ref()))
            .expect("Kubernetes objects serialize to JSON")
    }
}

fn api_version_of<K: Resource>(_: &K) -> &'static str {
    K::API_VERSION
}

fn kind_of<K: Resource>(_: &K) -> &'static str {
    K::KIND
}
