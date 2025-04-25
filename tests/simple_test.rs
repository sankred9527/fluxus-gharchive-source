


#[cfg(test)]
mod tests {

    use fluxus_gharchive_source::gharchive::{self, GHarchiveSource};
    use fluxus::sources::Source;
    use tokio::test;
    use fluxus::api::DataStream;
    use fluxus::api::io::CollectionSink;
    use std::time::Duration;
    use fluxus::utils::window::WindowConfig;
    use std::collections::HashMap;

    pub type EventTypeCount = HashMap<String, u32>;

    // Count the number of various event types.
    async fn kick_roll(source : GHarchiveSource) {
        let sink: CollectionSink<EventTypeCount> = CollectionSink::new();
        DataStream::new(source)                        
        .parallel(2)
        .window(WindowConfig::tumbling(Duration::from_millis(1000)))
        .aggregate(HashMap::new(), |mut counts, event| {
            *counts.entry(event.event_type).or_insert(0) += 1;
            counts
        })
        .sink(sink.clone())
        .await.expect("handle error");
        
        for result in sink.get_data() {
            let mut events: Vec<_> = result.iter().collect();
            println!("\nWindow results:");
            events.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
            for (event, count) in events {
                println!("  {}: {}", event, count);
            }
        }  
    }

    #[test]
    async fn test_eventtype_count() {
        
        let mut gh_source = gharchive::GHarchiveSource::new("tests/2015-01-01-15.json");
        gh_source.init().await.expect("open input file error");
        kick_roll(gh_source).await;

        // gziped file 
        let mut gh_source_gzip = gharchive::GHarchiveSource::new("tests/2025-01-01-15.json.gz");
        gh_source_gzip.init().await.expect("open input file error");
        kick_roll(gh_source_gzip).await;

    }
}
