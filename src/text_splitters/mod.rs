use anyhow::Result;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{cmp::max, fmt::Debug, sync::Arc};
use strum_macros::{Display, EnumString};
use thiserror::Error;

use crate::{EmbeddingGeneratorError, EmbeddingGeneratorTS};

#[derive(Error, Debug)]
pub enum TextSplitterError {
    #[error(transparent)]
    TokenizerError(#[from] EmbeddingGeneratorError),

    #[error("unknown splitter kind: `{0}`")]
    UnknownSplitterKind(String),
}

pub type TextSplitterTS = Arc<dyn TextSplitter + Send + Sync>;

#[derive(Display, Serialize, Deserialize, Clone, Debug, EnumString)]
pub enum TextSplitterKind {
    #[strum(serialize = "noop")]
    Noop,

    #[strum(serialize = "regex")]
    Regex { pattern: String },

    #[strum(serialize = "new_line")]
    NewLine,
}

async fn merge_tokens(
    tokens: Vec<u64>,
    max_tokens_per_chunk: u64,
    token_overlap: u64,
) -> Result<Vec<Vec<u64>>, TextSplitterError> {
    let step_size = max(
        max_tokens_per_chunk.checked_sub(token_overlap).unwrap_or(1),
        1,
    );

    let chunk_tokens: Vec<Vec<u64>> = (0..tokens.len())
        .step_by(step_size as usize)
        .map(|start_idx| {
            let end_idx = usize::min(start_idx + max_tokens_per_chunk as usize, tokens.len());
            tokens[start_idx..end_idx].to_vec()
        })
        .collect();
    Ok(chunk_tokens)
}

#[async_trait::async_trait]
pub trait TextSplitter {
    async fn split(
        &self,
        doc: &str,
        max_tokens: u64,
        max_token_overlap: u64,
    ) -> Result<Vec<String>, TextSplitterError>;

    async fn tokenize_encode(
        &self,
        input: Vec<String>,
    ) -> Result<Vec<Vec<u64>>, EmbeddingGeneratorError>;

    async fn tokenize_decode(
        &self,
        input: Vec<Vec<u64>>,
    ) -> Result<Vec<String>, EmbeddingGeneratorError>;
}

pub fn get_splitter(
    kind: TextSplitterKind,
    embedding_generator: EmbeddingGeneratorTS,
) -> Result<TextSplitterTS, TextSplitterError> {
    match kind {
        TextSplitterKind::NewLine => Ok(Arc::new(NewLineSplitter {
            embedding_generator,
        })),
        TextSplitterKind::Regex { pattern: p } => Ok(Arc::new(RegexSplitter {
            pattern: p,
            embedding_generator,
        })),
        TextSplitterKind::Noop => Ok(Arc::new(NoOpTextSplitter)),
    }
}

pub struct NewLineSplitter {
    embedding_generator: EmbeddingGeneratorTS,
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
        let tokens: Vec<Vec<u64>> = self.tokenize_encode(split_across_newlines).await?;
        let flattened_tokens = tokens.into_iter().flatten().collect();
        let chunk_tokens = merge_tokens(flattened_tokens, max_tokens, max_token_overlap).await?;
        self.tokenize_decode(chunk_tokens)
            .await
            .map_err(|e| e.into())
    }

    async fn tokenize_encode(
        &self,
        input: Vec<String>,
    ) -> Result<Vec<Vec<u64>>, EmbeddingGeneratorError> {
        self.embedding_generator.tokenize_encode(input).await
    }

    async fn tokenize_decode(
        &self,
        input: Vec<Vec<u64>>,
    ) -> Result<Vec<String>, EmbeddingGeneratorError> {
        self.embedding_generator.tokenize_decode(input).await
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

    async fn tokenize_encode(
        &self,
        _input: Vec<String>,
    ) -> Result<Vec<Vec<u64>>, EmbeddingGeneratorError> {
        Ok(vec![vec![]])
    }

    async fn tokenize_decode(
        &self,
        _input: Vec<Vec<u64>>,
    ) -> Result<Vec<String>, EmbeddingGeneratorError> {
        Ok(vec![])
    }
}

pub struct RegexSplitter {
    pub pattern: String,
    pub embedding_generator: EmbeddingGeneratorTS,
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
            let split_chunk_tokens =
                merge_tokens(flattened_tokens, max_tokens, max_token_overlap).await?;
            let split_chunks = self
                .tokenize_decode(split_chunk_tokens)
                .await
                .map_err(TextSplitterError::TokenizerError)?;
            chunks.extend(split_chunks);
        }

        Ok(chunks)
    }

    async fn tokenize_encode(
        &self,
        input: Vec<String>,
    ) -> Result<Vec<Vec<u64>>, EmbeddingGeneratorError> {
        self.embedding_generator.tokenize_encode(input).await
    }

    async fn tokenize_decode(
        &self,
        input: Vec<Vec<u64>>,
    ) -> Result<Vec<String>, EmbeddingGeneratorError> {
        self.embedding_generator.tokenize_decode(input).await
    }
}

#[cfg(test)]
mod tests {

    use std::fs;

    use crate::sentence_transformers;

    use super::*;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_basic_flat_xml() {
        let model = Arc::new(sentence_transformers::AllMiniLmL6V2::new().unwrap());
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
            embedding_generator: model,
        };
        let chunks = splitter.split(xml, 512, 0).await.unwrap();
        assert_eq!(chunks.len(), 5);
        // TODO fix the decoding of tokens to exclude all the whitespaces.
        assert_eq!(chunks[1], "< l id = 0 > about < / l >");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_new_line_splitter() {
        let model = Arc::new(sentence_transformers::AllMiniLmL6V2::new().unwrap());
        let splitter = get_splitter(TextSplitterKind::NewLine, model).unwrap();
        let doc = fs::read_to_string("./src/text_splitters/state_of_the_union.txt").unwrap();
        let chunks = splitter.split(&doc, 512, 0).await.unwrap();
        assert_eq!(chunks.len(), 19);

        let doc1 = "embiid is the mvp";
        let chunks1 = splitter.split(doc1, 512, 0).await.unwrap();
        assert_eq!(chunks1[0], doc1);
        assert_eq!(chunks1.len(), 1);
    }
}

impl From<crate::api::IndexDistance> for IndexDistance {
    fn from(value: IndexDistance) -> Self {
        match value {
            crate::api::IndexDistance::Dot => IndexDistance::Dot,
            crate::api::IndexDistance::Cosine => IndexDistance::Cosine,
            crate::api::IndexDistance::Euclidean => IndexDistance::Euclidean,
        }
    }
}

impl From<IndexDistance> for crate::api::IndexDistance {
    fn from(val: IndexDistance) -> Self {
        match val {
            IndexDistance::Dot => crate::api::IndexDistance::Dot,
            IndexDistance::Cosine => crate::api::IndexDistance::Cosine,
            IndexDistance::Euclidean => crate::api::IndexDistance::Euclidean,
        }
    }
}
