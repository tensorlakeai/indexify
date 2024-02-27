import { useLoaderData, LoaderFunctionArgs, redirect } from "react-router-dom";
import {
  Box,
  Typography,
  Stack,
  Breadcrumbs,
  TextField,
  Button,
  Alert,
} from "@mui/material";
import { IIndex, IndexifyClient, ISearchIndexResponse } from "getindexify";
import React, { useState } from "react";
import { Link } from "react-router-dom";
import SearchResultCard from "../../components/SearchResultCard";

export async function loader({ params }: LoaderFunctionArgs) {
  const namespace = params.namespace;
  const indexName = params.indexName;

  const client = await IndexifyClient.createClient();
  const indexes = (await client.indexes()).filter(
    (index) => index.name === indexName
  );
  if (!indexes.length) {
    return redirect("/");
  }

  return { index: indexes[0], namespace, client };
}

const ExtractionPolicyPage = () => {
  const { index, namespace, client } = useLoaderData() as {
    index: IIndex;
    namespace: string;
    client: IndexifyClient;
  };

  const [formData, setFormData] = useState<{ query: string; topK: number }>({
    query: "",
    topK: 3,
  });
  const [searchResults, setSearchResults] = useState<
    null | ISearchIndexResponse[]
  >(null);

  const onClickSearch = async () => {
    const results = await client.searchIndex(
      index.name,
      formData.query,
      formData.topK
    );
    setSearchResults(results);
  };

  return (
    <Stack direction="column" spacing={3}>
      <Breadcrumbs aria-label="breadcrumb">
        <Link color="inherit" to={`/${namespace}`}>
          {namespace}
        </Link>
        <Typography color="text.primary">Indexes</Typography>
        <Typography color="text.primary">{index.name}</Typography>
      </Breadcrumbs>
      <Box display={"flex"} alignItems={"center"}>
        <Typography variant="h2" component="h1">
          Extraction Policy - {index.name}
        </Typography>
      </Box>
      {/* Search */}
      <Box>
        <Stack direction={"row"} spacing={2}>
          <TextField
            label="Search Query"
            value={formData.query}
            onChange={(e) => {
              setFormData({ ...formData, query: e.currentTarget.value });
            }}
            variant="outlined"
            size="small"
          />
          <TextField
            label="topK"
            type="number"
            size="small"
            sx={{ width: "100px" }}
            value={formData.topK}
            inputProps={{
              min: 1,
              max: 9,
            }}
            onChange={(e) => {
              const minValue = 1;
              const maxValue = 9;
              let parsedValue = parseInt(e.target.value, 10);
              parsedValue = Math.max(Math.min(parsedValue, maxValue), minValue);
              setFormData({ ...formData, topK: parsedValue });
            }}
          />
          <Button
            disabled={formData.query.length === 0}
            onClick={onClickSearch}
            variant="contained"
          >
            Search
          </Button>
        </Stack>
      </Box>

      {searchResults !== null && (
        <Box>
          <Typography pb={2} variant="h3">
            Search Results ({searchResults.length})
          </Typography>
          <Stack direction={"column"}>
            {searchResults.length === 0 ? (
              <Alert variant="outlined" severity="info">
                No Content Found
              </Alert>
            ) : (
              searchResults.map((result) => {
                return <SearchResultCard namespace={namespace} data={result} />;
              })
            )}
          </Stack>
        </Box>
      )}
    </Stack>
  );
};

export default ExtractionPolicyPage;
