use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use serde_json::{json, Value};
use tessivum_core::{
    ContextHandle, Dependency, Entry, EntryId, EntryOptions, EntryTree, EventKey, EventOptions,
    Loader, LoaderFuture, LoaderRuntime, NativeConfigSchema, NativePlugin, NativePluginDescriptor,
    NativePluginFuture, NativePluginRuntime, PackageResolver, ResolvedPackage, RuntimeKind,
    ServiceKey,
};

#[derive(Clone)]
struct Greeting(&'static str);

struct Reloaded(&'static str);

type Log = Arc<Mutex<Vec<String>>>;

fn greeting_service() -> ServiceKey {
    ServiceKey::new("example.greeting", "native/v1")
}

fn reloaded_event() -> EventKey<Reloaded> {
    EventKey::new("example.reloaded")
}

fn provider_schema() -> NativeConfigSchema {
    NativeConfigSchema::Object {
        properties: BTreeMap::from([("label".into(), NativeConfigSchema::String)]),
        required: BTreeSet::from(["label".into()]),
        allow_additional: false,
    }
}

struct Provider {
    label: &'static str,
    log: Log,
}

impl NativePlugin for Provider {
    fn descriptor(&self) -> NativePluginDescriptor {
        NativePluginDescriptor {
            name: "native-minimal-provider".into(),
            version: self.label.into(),
            dependencies: Vec::new(),
            config_schema: provider_schema(),
        }
    }

    fn start<'a>(
        &'a mut self,
        context: ContextHandle,
        _config: &'a Value,
    ) -> NativePluginFuture<'a> {
        let label = self.label;
        let log = Arc::clone(&self.log);
        Box::pin(async move {
            context
                .provide(greeting_service(), Greeting(label))
                .expect("provider service belongs to its transaction");
            log.lock()
                .expect("example log is available")
                .push(format!("provider:{label}"));
            Ok(())
        })
    }

    fn update<'a>(
        &'a mut self,
        _context: ContextHandle,
        _config: &'a Value,
    ) -> NativePluginFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn stop<'a>(&'a mut self, _context: ContextHandle) -> NativePluginFuture<'a> {
        let label = self.label;
        let log = Arc::clone(&self.log);
        Box::pin(async move {
            log.lock()
                .expect("example log is available")
                .push(format!("stop:provider:{label}"));
            Ok(())
        })
    }
}

struct Consumer {
    log: Log,
}

impl NativePlugin for Consumer {
    fn descriptor(&self) -> NativePluginDescriptor {
        NativePluginDescriptor {
            name: "native-minimal-consumer".into(),
            version: "1.0.0".into(),
            dependencies: vec![Dependency::Required(greeting_service())],
            config_schema: NativeConfigSchema::Any,
        }
    }

    fn start<'a>(
        &'a mut self,
        context: ContextHandle,
        _config: &'a Value,
    ) -> NativePluginFuture<'a> {
        let log = Arc::clone(&self.log);
        Box::pin(async move {
            let greeting = context
                .get::<Greeting>(&greeting_service())
                .expect("provider lookup is valid")
                .expect("required provider is ready before consumer starts")
                .with(|service| service.0)
                .expect("provider remains current during consumer activation");
            log.lock()
                .expect("example log is available")
                .push(format!("consumer:{greeting}"));

            let event_log = Arc::clone(&log);
            context
                .events()
                .on(
                    &context.scope(),
                    reloaded_event(),
                    EventOptions {
                        global: true,
                        ..EventOptions::default()
                    },
                    move |event| {
                        event_log
                            .lock()
                            .expect("example log is available")
                            .push(format!("event:{}", event.0));
                        Ok(Value::Null)
                    },
                )
                .expect("consumer listener belongs to its transaction");
            Ok(())
        })
    }

    fn update<'a>(
        &'a mut self,
        _context: ContextHandle,
        _config: &'a Value,
    ) -> NativePluginFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn stop<'a>(&'a mut self, _context: ContextHandle) -> NativePluginFuture<'a> {
        let log = Arc::clone(&self.log);
        Box::pin(async move {
            log.lock()
                .expect("example log is available")
                .push("stop:consumer".into());
            Ok(())
        })
    }
}

struct Resolver;

impl PackageResolver for Resolver {
    fn resolve<'a>(
        &'a self,
        specifier: &'a str,
        _runtime: RuntimeKind,
    ) -> LoaderFuture<'a, ResolvedPackage> {
        let specifier = specifier.to_owned();
        Box::pin(async move {
            Ok(ResolvedPackage {
                location: specifier.clone(),
                specifier,
            })
        })
    }
}

fn entry(id: &str, package: &str, config: Value) -> Entry {
    Entry {
        package: package.into(),
        options: EntryOptions {
            id: EntryId::new(id).expect("example id is valid"),
            name: Some(id.into()),
            runtime: RuntimeKind::Native,
            config,
            inject: Vec::new(),
            isolate: Vec::new(),
            intercept: json!({}),
            disabled: false,
            group: None,
        },
    }
}

fn tree(provider: &str) -> EntryTree {
    EntryTree {
        // Consumer precedes provider deliberately: the descriptor gates activation.
        entries: vec![
            entry("consumer", "native/consumer", Value::Null),
            entry("provider", provider, json!({"label": provider})),
        ],
        groups: Vec::new(),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = NativePluginRuntime::new();
    runtime
        .register("native/provider-v1", {
            let log = Arc::clone(&log);
            move || Provider {
                label: "v1",
                log: Arc::clone(&log),
            }
        })
        .expect("provider v1 factory registers");
    runtime
        .register("native/provider-v2", {
            let log = Arc::clone(&log);
            move || Provider {
                label: "v2",
                log: Arc::clone(&log),
            }
        })
        .expect("provider v2 factory registers");
    runtime
        .register("native/consumer", {
            let log = Arc::clone(&log);
            move || Consumer {
                log: Arc::clone(&log),
            }
        })
        .expect("consumer factory registers");
    let runtime = Arc::new(runtime);
    let runtime_for_loader: Arc<dyn LoaderRuntime> = runtime.clone();
    let mut loader = Loader::try_new(Arc::new(Resolver), [runtime_for_loader])
        .expect("native runtime registers with the loader");

    loader
        .load(tree("native/provider-v1"))
        .await
        .expect("provider satisfies consumer dependency");
    loader
        .context()
        .events()
        .emit(&reloaded_event(), &Reloaded("v1"))
        .expect("typed event dispatches directly");

    loader
        .replace(tree("native/provider-v2"))
        .await
        .expect("provider replacement reloads the consumer");
    loader
        .context()
        .events()
        .emit(&reloaded_event(), &Reloaded("v2"))
        .expect("replacement consumer receives the event");

    let root = loader.context();
    loader
        .unload()
        .await
        .expect("all plugin instances quiesce before root disposal");
    root.scope()
        .dispose()
        .await
        .expect("root disposal succeeds");
    root.scope()
        .dispose()
        .await
        .expect("root disposal is idempotent");

    let log = log.lock().expect("example log is available");
    assert_eq!(
        *log,
        [
            "provider:v1",
            "consumer:v1",
            "event:v1",
            "stop:provider:v1",
            "stop:consumer",
            "provider:v2",
            "consumer:v2",
            "event:v2",
            "stop:provider:v2",
            "stop:consumer",
        ],
        "the complete native lifecycle is deterministic"
    );
    assert!(root.scope().effects().is_empty(), "root has zero resources");
    assert_eq!(runtime.snapshot().packages.len(), 3);

    println!("native minimal complete: provider replacement reloaded consumer; zero resources");
}
