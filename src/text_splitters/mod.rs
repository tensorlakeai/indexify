use anyhow::Result;
use regex::Regex;
use scraper::{Html, Selector};
use std::{cmp::max, fmt::Debug, sync::Arc};
use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum TokenizerError {
    #[error("unknown tokenizer: `{0}`")]
    UnknownTokenizer(String),
}

pub trait Tokenizer<T: Clone> {
    fn tokenize(&self, doc: &str) -> Result<Vec<T>, TokenizerError>;

    fn to_string(&self, tokens: Vec<T>) -> Result<String, TokenizerError>;
}

pub type TextSplitterTS = Arc<dyn TextSplitter<String> + Send + Sync>;

pub trait TextSplitter<T: Clone + Debug>: Tokenizer<T> {
    fn split(
        &self,
        doc: &str,
        max_tokens_per_chunk: usize,
        token_overlap: usize,
    ) -> Result<Vec<String>, TokenizerError> {
        let tokens = self.tokenize(doc)?;
        let step_size = max(
            max_tokens_per_chunk.checked_sub(token_overlap).unwrap_or(1),
            1,
        );

        (0..tokens.len())
            .step_by(step_size)
            .map(|start_idx| {
                let end_idx = usize::min(start_idx + max_tokens_per_chunk, tokens.len());
                self.to_string(tokens[start_idx..end_idx].to_vec())
            })
            .collect()
    }
}

pub fn get_splitter(splitter: &str) -> Result<TextSplitterTS, TokenizerError> {
    match splitter {
        "recursive" => Ok(Arc::new(SimpleTokenizer {
            separators: vec!["\n\n".into(), "\n".into(), " ".into(), "".into()],
        })),
        "new_line" => Ok(Arc::new(NewLineTokenizer)),
        "html" => Ok(Arc::new(HTMLSplitter)),
        "custom_dom" => Ok(Arc::new(CustomDomSplitter)),
        "noop" => Ok(Arc::new(NoOpTokenizer)),
        _ => Err(TokenizerError::UnknownTokenizer(splitter.to_owned())),
    }
}

pub struct NewLineTokenizer;

impl Tokenizer<String> for NewLineTokenizer {
    fn tokenize(&self, doc: &str) -> Result<Vec<String>, TokenizerError> {
        Ok(doc.split('\n').map(|s| s.to_owned()).collect())
    }

    fn to_string(&self, tokens: Vec<String>) -> Result<String, TokenizerError> {
        Ok(tokens.join("\n"))
    }
}

impl TextSplitter<String> for NewLineTokenizer {}

pub struct SimpleTokenizer {
    separators: Vec<String>,
}

impl Tokenizer<String> for SimpleTokenizer {
    fn tokenize(&self, text: &str) -> Result<Vec<String>, TokenizerError> {
        let mut texts = vec![text.to_owned()];
        for sep in &self.separators {
            let mut new_texts = vec![];
            for text in texts {
                if sep.is_empty() {
                    new_texts.extend(text.chars().map(|c| c.to_string()).collect::<Vec<String>>());
                } else {
                    new_texts.extend(
                        text.split(sep)
                            .map(|s| s.to_owned())
                            .collect::<Vec<String>>(),
                    );
                }
            }
            texts = new_texts;
        }
        Ok(texts)
    }

    fn to_string(&self, tokens: Vec<String>) -> Result<String, TokenizerError> {
        Ok(tokens.join(""))
    }
}

impl TextSplitter<String> for SimpleTokenizer {}

pub struct HTMLSplitter;

impl Tokenizer<String> for HTMLSplitter {
    fn tokenize(&self, doc: &str) -> Result<Vec<String>, TokenizerError> {
        let parsed_doc = Html::parse_document(doc);
        let element_selector = Selector::parse("*").unwrap();
        let mut tokens: Vec<String> = vec![];
        for element in parsed_doc.select(&element_selector) {
            tokens.push(element.text().collect::<String>());
        }
        Ok(tokens)
    }
    fn to_string(&self, tokens: Vec<String>) -> Result<String, TokenizerError> {
        Ok(tokens.join(""))
    }
}

impl TextSplitter<String> for HTMLSplitter {}

pub struct CustomDomSplitter;
impl CustomDomSplitter {
    fn split_by_closing_selectors(input: &str) -> Vec<String> {
        let closing_tag_pattern = Regex::new(r"</[^>]+>").unwrap();
        let mut result: Vec<String> = Vec::new();
        let mut start_index = 0;

        for mat in closing_tag_pattern.find_iter(input) {
            let end_index = mat.end();
            let part = &input[(start_index + 1)..end_index];
            result.push(part.to_string());
            start_index = end_index;
        }

        result
    }
}

impl Tokenizer<String> for CustomDomSplitter {
    fn tokenize(&self, doc: &str) -> Result<Vec<String>, TokenizerError> {
        let tokens = CustomDomSplitter::split_by_closing_selectors(doc);
        Ok(tokens)
    }
    fn to_string(&self, tokens: Vec<String>) -> Result<String, TokenizerError> {
        Ok(tokens.join(""))
    }
}

impl TextSplitter<String> for CustomDomSplitter {}

pub struct NoOpTokenizer;

impl Tokenizer<String> for NoOpTokenizer {
    fn tokenize(&self, doc: &str) -> Result<Vec<String>, TokenizerError> {
        Ok(vec![doc.to_owned()])
    }
    fn to_string(&self, tokens: Vec<String>) -> Result<String, TokenizerError> {
        Ok(tokens.join(""))
    }
}

impl TextSplitter<String> for NoOpTokenizer {}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_basic_flat_xml() {
        let xml = r#"
<a id=7 role=combobox title=Search type=search aria-label=Search> </a>
<l id=0>About</l>
<l id=1>Store</l>
<l id=2 aria-label=Gmail (opens a new tab)>Gmail</l>
<l id=3 aria-label=Search for Images (opens a new tab)>Images</l>
<b id=4 aria-label=Google apps/>
"#;
        let splitter = super::CustomDomSplitter {};
        let tokens = splitter.tokenize(xml).unwrap();
        assert_eq!(tokens.len(), 5);
        assert_eq!(tokens[1], "<l id=0>About</l>");
    }

    #[test]
    fn test_char_splitter() {
        let doc1: String = "foo bar baz a a".into();
        let splitter = super::SimpleTokenizer {
            separators: vec![" ".into()],
        };
        let splitter_tokens = splitter.tokenize(&doc1).unwrap();
        assert_eq!(5, splitter_tokens.len());
        let result = splitter.split(&doc1, 3, 1).unwrap();
        assert_eq!(result, vec!["foobarbaz", "bazaa", "a"]);
    }

    #[test]
    fn test_recursive_text_splitter() {
        let doc1: String = "foo bar baz a a".into();
        let splitter = get_splitter("recursive").unwrap();
        let splitter_tokens = splitter.tokenize(&doc1).unwrap();
        assert_eq!(11, splitter_tokens.len());
        let result = splitter.split(&doc1, 3, 1).unwrap();
        assert_eq!(result, vec!["foo", "oba", "arb", "baz", "zaa", "a"]);
    }
}
