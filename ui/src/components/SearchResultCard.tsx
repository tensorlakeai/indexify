import { Box, Chip, Divider, Paper, Typography } from "@mui/material";
import { ISearchIndexResponse } from "getindexify";
import { Link } from "react-router-dom";

const DisplayData = ({
  label,
  value,
}: {
  label: string;
  value: string | number;
}) => {
  return (
    <Box display="flex" flexDirection={"row"} alignItems={"center"}>
      <Typography sx={{ fontSize: "14px" }} variant="body1">
        {label}:{" "}
      </Typography>
      <Typography sx={{ fontSize: "14px" }} pl={1} variant="label">
        {value}
      </Typography>
    </Box>
  );
};

const SearchResultCard = ({
  data,
  namespace,
}: {
  data: ISearchIndexResponse;
  namespace: string;
}) => {
  return (
    <Paper elevation={3} sx={{ padding: 2, marginBottom: 2 }}>
      <Typography variant="h4">
        Content ID: <Link to={`/${namespace}/content/${data.content_id}`} target="_blank">{data.content_id}</Link>
      </Typography>
      <Divider sx={{ my: 1 }} />
      <Typography py={1} variant="body1">
        {data.text}
      </Typography>
      <Divider sx={{ my: 1 }} />
      <DisplayData label="Confidence Score" value={data.confidence_score} />
      {Object.keys(data.labels).length !== 0 && (
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
