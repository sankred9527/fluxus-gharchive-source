use async_trait::async_trait;
use fluxus::utils::models::{Record, StreamResult};
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use fluxus::sources::Source;
use chrono;
use async_compression::tokio::bufread::GzipDecoder;



use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub public: bool,
    pub payload: Value,
    pub repo: Repo,
    pub actor: Actor,
    pub org: Option<Org>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Repo {
    pub id: i64,
    pub name: String,
    pub url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Actor {
    pub id: i64,
    pub login: String,
    pub gravatar_id: String,
    pub avatar_url: String,
    pub url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Org {
    pub id: i64,
    pub login: String,
    pub gravatar_id: String,
    pub avatar_url: String,
    pub url: String,
}


/// A source that reads github archive source files
pub struct GHarchiveSource {
    path: PathBuf,
    is_gzip : bool,
    reader: Option<Box<dyn tokio::io::AsyncBufRead + Unpin + Send + Sync>>,
}

impl GHarchiveSource {
    /// Create a new github archive source from a file path
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {                
        let path_info = path.into();
        let ext_name = path_info.extension().and_then(|s| s.to_str());        
        
        let is_gzip = if Some("gz") == ext_name {
            true
        } else {
            false
        };

        Self {            
            is_gzip,
            path: path_info,
            reader: None,
        }
    }
}

#[async_trait]
impl Source<Event> for GHarchiveSource {
    async fn init(&mut self) -> StreamResult<()> {
        let file = File::open(&self.path).await?;                

        let mut foo: Box<dyn tokio::io::AsyncBufRead + Unpin + Send>;
        if self.is_gzip {
            let buf_reader = BufReader::new(file);
            let decompressed = BufReader::new(GzipDecoder::new(buf_reader));
            self.reader = Some(Box::new(decompressed));
        } else {            
            self.reader = Some(Box::new(BufReader::new(file)));
        }
        Ok(())
    }

    /*
     archive download from https://data.gharchive.org/ must be split by CRLF
     */
    async fn next(&mut self) -> StreamResult<Option<Record<Event>>> {
        if let Some(reader) = &mut self.reader {
            let mut line = String::new();            
            match reader.read_line(&mut line).await {
                Ok(0) => Ok(None), // EOF
                Ok(_) => {
                    let line = line.trim().to_string();

                    let event: Event = serde_json::from_str(&line).unwrap();
                    //println!("{:#?}", event);                    

                    Ok(Some(Record::new(event)))
                }
                Err(e) => Err(e.into()),
            }
        } else {
            Ok(None)
        }
    }

    async fn close(&mut self) -> StreamResult<()> {
        self.reader = None;
        Ok(())
    }
}
