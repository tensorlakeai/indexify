import * as React from "react";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import DataObjectIcon from "@mui/icons-material/DataObject";
import MemoryIcon from "@mui/icons-material/Memory";

export const mainListItems = (
  <React.Fragment>
    <ListItemButton href="/repositories">
      <ListItemIcon>
        <DataObjectIcon />
      </ListItemIcon>
      <ListItemText primary="Namespaces" />
    </ListItemButton>
    <ListItemButton href="/extractors">
      <ListItemIcon>
        <MemoryIcon />
      </ListItemIcon>
      <ListItemText primary="Extractors" />
    </ListItemButton>
  </React.Fragment>
);

export const secondaryListItems = <React.Fragment></React.Fragment>;
