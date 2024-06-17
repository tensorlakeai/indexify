import math

from rapidfuzz import fuzz
import re
import regex
from statistics import mean

CHUNK_MIN_CHARS = 25

def chunk_text(text, chunk_len=500):
    chunks = [text[i:i+chunk_len] for i in range(0, len(text), chunk_len)]
    chunks = [c for c in chunks if c.strip() and len(c) > CHUNK_MIN_CHARS]
    return chunks


def overlap_score(hypothesis_chunks, reference_chunks):
    length_modifier = len(hypothesis_chunks) / len(reference_chunks)
    search_distance = max(len(reference_chunks) // 5, 10)
    chunk_scores = []
    for i, hyp_chunk in enumerate(hypothesis_chunks):
        max_score = 0
        total_len = 0
        i_offset = int(i * length_modifier)
        chunk_range = range(max(0, i_offset-search_distance), min(len(reference_chunks), i_offset+search_distance))
        for j in chunk_range:
            ref_chunk = reference_chunks[j]
            score = fuzz.ratio(hyp_chunk, ref_chunk, score_cutoff=30) / 100
            if score > max_score:
                max_score = score
                total_len = len(ref_chunk)
        chunk_scores.append(max_score)
    return chunk_scores


def score_text(hypothesis, reference):
    # Returns a 0-1 alignment score
    hypothesis_chunks = chunk_text(hypothesis)
    reference_chunks = chunk_text(reference)
    chunk_scores = overlap_score(hypothesis_chunks, reference_chunks)
    return mean(chunk_scores)


import os
import time
from marker.markdown_extractor import MarkdownExtractorConfig, MarkdownExtractor
from pdf.pdf_extractor import PDFExtractorConfig, PDFExtractor
from unstructuredio.unstructured_pdf import UnstructuredIOConfig, UnstructuredIOExtractor
from indexify_extractor_sdk import Content

# Initialize extractors
markdown_extractor = MarkdownExtractor()
pdf_extractor = PDFExtractor()
unstructured_extractor = UnstructuredIOExtractor()

# Function to extract text content from extractor output
def extract_text_content(result):
    if isinstance(result, list):
        result = result[0]
    return result.data.decode('utf-8')

# Function to run Marker extractor
def use_marker(pdf_filepath):
    with open(pdf_filepath, "rb") as f:
        pdf_data = f.read()
    content = Content(content_type="application/pdf", data=pdf_data)
    config = MarkdownExtractorConfig(output_types=["text"])
    result = markdown_extractor.extract(content, config)
    return extract_text_content(result)

# Function to run PDF extractor
def use_pdf_extractor(pdf_filepath):
    with open(pdf_filepath, "rb") as f:
        pdf_data = f.read()
    content = Content(content_type="application/pdf", data=pdf_data)
    config = PDFExtractorConfig(output_types=["text"])
    result = pdf_extractor.extract(content, config)
    return extract_text_content(result)

# Function to run Unstructured IO extractor
def use_unstructured(pdf_filepath):
    with open(pdf_filepath, "rb") as f:
        pdf_data = f.read()
    content = Content(content_type="application/pdf", data=pdf_data)
    config = UnstructuredIOConfig(output_types=["text"])
    result = unstructured_extractor.extract(content, config)
    return extract_text_content(result)

# Directories containing PDF and reference files
pdf_dir = "/content/pdfs"
reference_dir = "/content/references"

# Iterate over PDF files in the directory
for pdf_file in os.listdir(pdf_dir):
    if pdf_file.endswith(".pdf"):
        pdf_path = os.path.join(pdf_dir, pdf_file)
        reference_file = os.path.join(reference_dir, pdf_file.replace(".pdf", ".md"))

        # Read reference text
        with open(reference_file, "r", encoding="utf-8") as f:
            reference_text = f.read()

        # Run Marker extractor
        start_time = time.time()
        marker_output = use_marker(pdf_path)
        marker_time = time.time() - start_time
        marker_score = score_text(marker_output, reference_text)
        print(f"Marker score for {pdf_file}: {marker_score} (Time taken: {marker_time:.2f} seconds)")

        # Run PDF extractor
        start_time = time.time()
        pdf_extractor_output = use_pdf_extractor(pdf_path)
        pdf_extractor_time = time.time() - start_time
        pdf_extractor_score = score_text(pdf_extractor_output, reference_text)
        print(f"PDF extractor score for {pdf_file}: {pdf_extractor_score} (Time taken: {pdf_extractor_time:.2f} seconds)")

        # Run Unstructured IO extractor
        start_time = time.time()
        unstructured_output = use_unstructured(pdf_path)
        unstructured_time = time.time() - start_time
        unstructured_score = score_text(unstructured_output, reference_text)
        print(f"Unstructured IO score for {pdf_file}: {unstructured_score} (Time taken: {unstructured_time:.2f} seconds)")
