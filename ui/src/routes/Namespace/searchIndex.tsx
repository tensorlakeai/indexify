import { useLoaderData, LoaderFunctionArgs, redirect } from "react-router-dom";
import {
  Box,
  Typography,
  Stack,
  Breadcrumbs,
  TextField,
  Button,
  Alert,
  CircularProgress,
} from "@mui/material";
import { IIndex, IndexifyClient, ISearchIndexResponse } from "getindexify";
import React, { useState } from "react";
import { Link } from "react-router-dom";
import SearchResultCard from "../../components/SearchResultCard";
import { getIndexifyServiceURL } from "../../utils/helpers";
import { AxiosError, isAxiosError } from "axios";

export async function loader({ params }: LoaderFunctionArgs) {
  const namespace = params.namespace;
  const indexName = params.indexName;

  const client = await IndexifyClient.createClient({
    serviceUrl: getIndexifyServiceURL(),
    namespace,
  });
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
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<AxiosError | null>(null);

  const [searchResults, setSearchResults] = useState<
    null | ISearchIndexResponse[]
  >(null);

  const onClickSearch = async () => {
    setLoading(true);
    await client
      .searchIndex(index.name, formData.query, formData.topK)
      .then((results) => {
        setError(null);
        setSearchResults(results);
      })
      .catch((e) => {
        if (isAxiosError(e)) {
          setError(e);
        }
      })
      .finally(() => {
        setLoading(false);
      });
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
          Search Index - {index.name}
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
            disabled={loading || formData.query.length === 0}
            onClick={onClickSearch}
            variant="contained"
          >
            Search
          </Button>
        </Stack>
      </Box>
      {loading && (
        <Box sx={{ display: "flex" }}>
          <CircularProgress />
        </Box>
      )}
      {error && <Alert severity="error">Error: {error.message} - {String(error.response?.data || "unknown")}</Alert>}
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
                return <SearchResultCard key={result.content_id} namespace={namespace} data={result} />;
              })
            )}
          </Stack>
        </Box>
      )}
    </Stack>
  );
};

export default ExtractionPolicyPage;
