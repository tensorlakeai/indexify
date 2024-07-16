import { Box, Chip, Divider, Paper, Typography } from "@mui/material";
import { ISearchIndexResponse } from "getindexify";
import { Link } from "react-router-dom";

const DisplayData = ({
  label,
  value
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
  namespace
}: {
  data: ISearchIndexResponse;
  namespace: string;
}) => {
  return (
    <Paper
      sx={{
        paddingTop: 2,
        paddingBottom: 2,
        marginBottom: 2,
        boxShadow: "0px 0px 2px 0px #D0D6DE",
        display: "flex",
        flexDirection: "column",
        borderRadius: "12px"
      }}
    >
      <Box display={"flex"} flexDirection={"column"} ml={2} mr={2} mb={2}>
        <Typography variant="overline">CONTENT ID: </Typography>
        <Link
          to={`/${namespace}/extraction-graphs/${data.root_content_metadata?.extraction_graph_names[0]}/content/${data.content_id}`}
          target="_blank"
          style={{ color: "#1C2026", fontWeight: 500 }}
        >
          {data.content_id}
        </Link>
      </Box>
      <Divider variant="fullWidth" />
      <Box
        display={"flex"}
        flexDirection={"row"}
        alignItems={"center"}
        mt={2}
        ml={2}
        mr={2}
      >
        <DisplayData label="Confidence Score" value={data.confidence_score} />
        {Object.keys(data.labels).length !== 0 &&
          <Box display={"flex"} gap={1} ml={4} alignItems={"center"}>
            {Object.keys(data.labels).map((val: string) => {
              return (
                <Chip
                  key={val}
                  label={`${val}:${data.labels[val]}`}
                  sx={{ backgroundColor: "#E5EFFB", color: "#1C2026" }}
                />
              );
            })}
          </Box>}
      </Box>
    </Paper>
  );
};

export default SearchResultCard;
