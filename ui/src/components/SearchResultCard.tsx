import { Box, Chip, Divider, Paper, Typography } from "@mui/material";
import { ISearchIndexResponse } from "getindexify";

interface SearchResultCardProps {
  data: ISearchIndexResponse;
}

const SearchResultCard = ({ data }: SearchResultCardProps) => {
  data.labels = { test: "one" };
  return (
    <Paper elevation={3} sx={{ padding: 2, marginBottom: 2 }}>
      <Typography variant="body1">{data.text}</Typography>
      <Divider sx={{ my: 1 }} />
      <Typography
        variant="body2"
        sx={{ wordBreak: "break-all", maxHeight: "300px", overflowY: "scroll" }}
      >
        Confidence Score: {data.confidence_score}
      </Typography>
      {Object.keys(data.labels).length && (
        <Box pt={1}>
          {Object.keys(data.labels).map((val: string) => {
            return <Chip key={val} label={`${val}:${data.labels[val]}`} />;
          })}
        </Box>
      )}
    </Paper>
  );
};

export default SearchResultCard;
