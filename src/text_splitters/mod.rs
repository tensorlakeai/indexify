use anyhow::Result;
use std::cmp::max;

pub trait Tokenizer<T: Clone> {
    fn tokenize(&self, doc: &str) -> Result<Vec<T>>;

    fn to_string(&self, tokens: Vec<T>) -> Result<String>;
}

pub trait TextSplitter<T: Clone>: Tokenizer<T> {
    fn split(
        &self,
        doc: &str,
        max_tokens_per_chunk: usize,
        token_overlap: usize,
    ) -> Result<Vec<String>> {
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
