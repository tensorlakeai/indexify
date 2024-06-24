import { useLoaderData } from 'react-router-dom'
import {
  Box,
  Typography,
  Stack,
  Breadcrumbs,
  Button,
  Alert,
  CircularProgress,
  Chip,
  OutlinedInput,
} from '@mui/material'
import { IIndex, IndexifyClient, ISearchIndexResponse } from 'getindexify'
import { useState } from 'react'
import { Link } from 'react-router-dom'
import SearchResultCard from '../../components/SearchResultCard'
import { AxiosError, isAxiosError } from 'axios';
import NavigateNextIcon from '@mui/icons-material/NavigateNext';

const SearchIndexPage = () => {
  const { index, namespace, client } = useLoaderData() as {
    index: IIndex
    namespace: string
    client: IndexifyClient
  }

  const [formData, setFormData] = useState<{ query: string; topK: number }>({
    query: '',
    topK: 3,
  })
  const [loading, setLoading] = useState<boolean>(false)
  const [error, setError] = useState<AxiosError | null>(null)

  const [searchResults, setSearchResults] = useState<
    null | ISearchIndexResponse[]
  >(null)

  const onClickSearch = async () => {
    setLoading(true)
    await client
      .searchIndex(index.name, formData.query, formData.topK)
      .then((results) => {
        setError(null)
        setSearchResults(results)
      })
      .catch((e) => {
        if (isAxiosError(e)) {
          setError(e)
        }
      })
      .finally(() => {
        setLoading(false)
      })
  }

  return (
    <Stack direction="column" spacing={2}>
      <Breadcrumbs aria-label="breadcrumb" separator={<NavigateNextIcon fontSize="small" />}>
        <Typography color="text.primary">{namespace}</Typography>
        <Link color="inherit" to={`/${namespace}/indexes`}>
          <Typography color="text.primary">Indexes</Typography>
        </Link>
        <Typography color="text.primary">{index.name}</Typography>
      </Breadcrumbs>
      <Box display={'flex'} alignItems={'center'} justifyContent={'space-between'}>
        <Typography variant="h6" gutterBottom>
          Search Indexes of <Chip sx={{borderRadius: '50px', fontWeight: 400, color: '#4A4F56'}} label={index.name}/>
        </Typography>
        <Stack direction={'row'} spacing={2} alignItems={'center'}>
          <Typography variant="caption" gutterBottom>Search Query: </Typography>
          <OutlinedInput
            placeholder="Search Query"
            value={formData.query}
            onChange={(e) => {
              setFormData({ ...formData, query: e.currentTarget.value })
            }}
            notched={false}
            sx={{ width: '200px',backgroundColor: "white", height: '2rem'}}
            size="small" 
          />
          <Typography variant="caption" gutterBottom>topK: </Typography>
          <OutlinedInput
            placeholder="topK"
            type="number"
            size="small"
            sx={{ width: '100px', backgroundColor: "white", height: '2rem' }}
            value={formData.topK}
            inputProps={{
              min: 1,
              max: 9,
            }}
            onChange={(e) => {
              const minValue = 1
              const maxValue = 9
              let parsedValue = parseInt(e.target.value, 10)
              parsedValue = Math.max(Math.min(parsedValue, maxValue), minValue)
              setFormData({ ...formData, topK: parsedValue })
            }}
            notched={false}
          />
          <Button
            disabled={loading || formData.query.length === 0}
            onClick={onClickSearch}
            variant="contained"
            sx={{ height: '2rem' }}
          >
            Search
          </Button>
        </Stack>
      </Box>
      {loading && (
        <Box sx={{ display: 'flex' }}>
          <CircularProgress />
        </Box>
      )}
      {error && (
        <Alert severity="error">
          Error: {error.message} - {String(error.response?.data || 'unknown')}
        </Alert>
      )}
      {searchResults !== null && (
        <Box>
          <Typography pb={2} variant="subtitle1" sx={{ color: "#4A4F56" }}>
            Search Results <Chip sx={{ borderRadius: '100px', fontWeight: 400, backgroundColor: '#E5EFFB', color: '#1C2026' }} label={searchResults.length}/> 
          </Typography>
          <Stack direction={'column'}>
            {searchResults.length === 0 ? (
              <Alert variant="outlined" severity="info">
                No Content Found
              </Alert>
            ) : (
              searchResults.map((result) => {
                return (
                  <SearchResultCard
                    key={result.content_id}
                    namespace={namespace}
                    data={result}
                  />
                )
              })
            )}
          </Stack>
        </Box>
      )}
    </Stack>
  )
}

export default SearchIndexPage;
