use anyhow::Result;
use regex::Regex;
use std::{cmp::max, fmt::Debug, sync::Arc};
use strum_macros::{Display, EnumString};
use thiserror::Error;

use crate::{EmbeddingGeneratorError, EmbeddingGeneratorTS};

#[derive(Error, Debug)]
pub enum TextSplitterError {
    #[error(transparent)]
    TokenizerError(#[from] EmbeddingGeneratorError),
}

pub type TextSplitterTS = Arc<dyn TextSplitter + Send + Sync>;

#[derive(Display, Clone, Debug, EnumString)]
pub enum TextSplitterKind {
    #[strum(serialize = "noop")]
    Noop,

    #[strum(serialize = "regex")]
    Regex { pattern: String },

    #[strum(serialize = "new_line")]
    NewLine,
}

#[async_trait::async_trait]
pub trait TextSplitter {
    async fn split(
        &self,
        doc: &str,
        max_tokens: u64,
        max_token_overlap: u64,
    ) -> Result<Vec<String>, TextSplitterError>;

    async fn merge(
        &self,
        tokens: Vec<i64>,
        max_tokens_per_chunk: u64,
        token_overlap: u64,
    ) -> Result<Vec<String>, TextSplitterError> {
        let step_size = max(
            max_tokens_per_chunk.checked_sub(token_overlap).unwrap_or(1),
            1,
        );

        let chunk_tokens: Vec<Vec<i64>> = (0..tokens.len())
            .step_by(step_size as usize)
            .map(|start_idx| {
                let end_idx = usize::min(start_idx + max_tokens_per_chunk as usize, tokens.len());
                tokens[start_idx..end_idx].to_vec()
            })
            .collect();

        self.tokenize_decode(chunk_tokens)
            .await
            .map_err(|e| e.into())
    }

    async fn tokenize(
        &self,
        input: Vec<String>,
    ) -> Result<Vec<Vec<String>>, EmbeddingGeneratorError>;

    async fn tokenize_encode(
        &self,
        input: Vec<String>,
    ) -> Result<Vec<Vec<i64>>, EmbeddingGeneratorError>;

    async fn tokenize_decode(
        &self,
        input: Vec<Vec<i64>>,
    ) -> Result<Vec<String>, EmbeddingGeneratorError>;
}

pub fn get_splitter(
    kind: TextSplitterKind,
    embedding_router: EmbeddingGeneratorTS,
    model: String,
) -> Result<TextSplitterTS, TextSplitterError> {
    match kind {
        TextSplitterKind::NewLine => Ok(Arc::new(NewLineSplitter {
            embedding_router,
            model,
        })),
        TextSplitterKind::Regex { pattern: p } => Ok(Arc::new(RegexSplitter {
            pattern: p,
            embedding_router,
            model,
        })),
        TextSplitterKind::Noop => Ok(Arc::new(NoOpTextSplitter)),
    }
}

pub struct NewLineSplitter {
    embedding_router: EmbeddingGeneratorTS,
    model: String,
}

#[async_trait::async_trait]
impl TextSplitter for NewLineSplitter {
    async fn split(
        &self,
        doc: &str,
        max_tokens: u64,
        max_token_overlap: u64,
    ) -> Result<Vec<String>, TextSplitterError> {
        let split_across_newlines = doc
            .split('\n')
            .map(|s| s.to_owned())
            .collect::<Vec<String>>();
        let tokens: Vec<Vec<i64>> = self.tokenize_encode(split_across_newlines).await?;
        let flattened_tokens = tokens.into_iter().flatten().collect();
        let chunks = self
            .merge(flattened_tokens, max_tokens, max_token_overlap)
            .await?;
        Ok(chunks)
    }

    async fn tokenize(
        &self,
        inputs: Vec<String>,
    ) -> Result<Vec<Vec<String>>, EmbeddingGeneratorError> {
        self.embedding_router
            .tokenize_text(inputs, self.model.clone())
            .await
    }

    async fn tokenize_encode(
        &self,
        input: Vec<String>,
    ) -> Result<Vec<Vec<i64>>, EmbeddingGeneratorError> {
        self.embedding_router
            .tokenize_encode(input, self.model.clone())
            .await
    }

    async fn tokenize_decode(
        &self,
        input: Vec<Vec<i64>>,
    ) -> Result<Vec<String>, EmbeddingGeneratorError> {
        self.embedding_router
            .tokenize_decode(input, self.model.clone())
            .await
    }
}

pub struct NoOpTextSplitter;

#[async_trait::async_trait]
impl TextSplitter for NoOpTextSplitter {
    async fn split(
        &self,
        doc: &str,
        _max_tokens: u64,
        _max_token_overlap: u64,
    ) -> Result<Vec<String>, TextSplitterError> {
        Ok(vec![doc.to_owned()])
    }

    async fn tokenize(
        &self,
        input: Vec<String>,
    ) -> Result<Vec<Vec<String>>, EmbeddingGeneratorError> {
        Ok(vec![input])
    }

    async fn tokenize_encode(
        &self,
        _input: Vec<String>,
    ) -> Result<Vec<Vec<i64>>, EmbeddingGeneratorError> {
        Ok(vec![vec![]])
    }

    async fn tokenize_decode(
        &self,
        _input: Vec<Vec<i64>>,
    ) -> Result<Vec<String>, EmbeddingGeneratorError> {
        Ok(vec![])
    }
}

pub struct RegexSplitter {
    pub pattern: String,
    pub embedding_router: EmbeddingGeneratorTS,
    pub model: String,
}

#[async_trait::async_trait]
impl TextSplitter for RegexSplitter {
    async fn split(
        &self,
        doc: &str,
        max_tokens: u64,
        max_token_overlap: u64,
    ) -> Result<Vec<String>, TextSplitterError> {
        let closing_tag_pattern = Regex::new(&self.pattern).unwrap();
        let mut splits: Vec<String> = Vec::new();
        let mut start_index = 0;

        for mat in closing_tag_pattern.find_iter(doc) {
            let end_index = mat.end();
            let part = &doc[(start_index + 1)..end_index];
            splits.push(part.to_string());
            start_index = end_index;
        }
        let mut chunks: Vec<String> = Vec::new();
        for split in splits {
            let tokens = self.tokenize_encode(vec![split]).await?;
            let flattened_tokens = tokens.into_iter().flatten().collect();
            let split_chunks = self
                .merge(flattened_tokens, max_tokens, max_token_overlap)
                .await?;
            chunks.extend(split_chunks);
        }

        Ok(chunks)
    }

    async fn tokenize(
        &self,
        input: Vec<String>,
    ) -> Result<Vec<Vec<String>>, EmbeddingGeneratorError> {
        self.embedding_router
            .tokenize_text(input, self.model.clone())
            .await
    }

    async fn tokenize_encode(
        &self,
        input: Vec<String>,
    ) -> Result<Vec<Vec<i64>>, EmbeddingGeneratorError> {
        self.embedding_router
            .tokenize_encode(input, self.model.clone())
            .await
    }

    async fn tokenize_decode(
        &self,
        input: Vec<Vec<i64>>,
    ) -> Result<Vec<String>, EmbeddingGeneratorError> {
        self.embedding_router
            .tokenize_decode(input, self.model.clone())
            .await
    }
}

#[cfg(test)]
mod tests {

    use std::fs;

    use crate::{EmbeddingRouter, ServerConfig};

    use super::*;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_basic_flat_xml() {
        let embedding_router =
            Arc::new(EmbeddingRouter::new(Arc::new(ServerConfig::default())).unwrap());
        let xml = r#"
    <a id=7 role=combobox title=Search type=search aria-label=Search> </a>
    <l id=0>About</l>
    <l id=1>Store</l>
    <l id=2 aria-label=Gmail (opens a new tab)>Gmail</l>
    <l id=3 aria-label=Search for Images (opens a new tab)>Images</l>
    <b id=4 aria-label=Google apps/>
    "#;
        let splitter = super::RegexSplitter {
            pattern: r"<\/[^>]+>".into(),
            embedding_router,
            model: "all-minilm-l12-v2".into(),
        };
        let chunks = splitter.split(xml, 512, 0).await.unwrap();
        assert_eq!(chunks.len(), 5);
        // TODO fix the decoding of tokens to exclude all the whitespaces.
        assert_eq!(chunks[1], "< l id = 0 > about < / l >");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_new_line_splitter() {
        let embedding_router =
            Arc::new(EmbeddingRouter::new(Arc::new(ServerConfig::default())).unwrap());
        let splitter = get_splitter(
            TextSplitterKind::NewLine,
            embedding_router,
            "all-minilm-l12-v2".into(),
        )
        .unwrap();
        let doc = fs::read_to_string("./src/text_splitters/state_of_the_union.txt").unwrap();
        let chunks = splitter.split(&doc, 512, 0).await.unwrap();
        assert_eq!(chunks.len(), 19);

        let doc1 = "embiid is the mvp";
        let chunks1 = splitter.split(&doc1, 512, 0).await.unwrap();
        assert_eq!(chunks1[0], doc1);
        assert_eq!(chunks1.len(), 1);
    }
}
