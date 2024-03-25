import * as React from "react";
import { styled, ThemeProvider } from "@mui/material/styles";
import CssBaseline from "@mui/material/CssBaseline";
import Box from "@mui/material/Box";
import MuiAppBar, { AppBarProps as MuiAppBarProps } from "@mui/material/AppBar";
import Toolbar from "@mui/material/Toolbar";
import Typography from "@mui/material/Typography";
import Container from "@mui/material/Container";
import Link from "@mui/material/Link";
import {
  LoaderFunctionArgs,
  Outlet,
  redirect,
  useLoaderData,
} from "react-router-dom";
import theme from "../theme";
import { Stack } from "@mui/system";
import MenuItem from "@mui/material/MenuItem";
import Menu from "@mui/material/Menu";
import { Button } from "@mui/material";
import { IndexifyClient } from "getindexify";
import DataObjectIcon from "@mui/icons-material/DataObject";
import CircleIcon from "@mui/icons-material/Circle";
import { getIndexifyServiceURL, stringToColor } from "../utils/helpers";

export async function loader({ params }: LoaderFunctionArgs) {
  const namespaces = (
    await IndexifyClient.namespaces({
      serviceUrl: getIndexifyServiceURL(),
    })
  ).map((repo) => repo.name);

  if (!params.namespace || !namespaces.includes(params.namespace)) {
    if (params.namespace !== "default") {
      return redirect(`/${namespaces[0] ?? "default"}`);
    }
  }
  return { namespaces, namespace: params.namespace };
}

function Copyright(props: any) {
  return (
    <Typography
      variant="body2"
      color="text.secondary"
      align="center"
      {...props}
    >
      {"Copyright © "}
      <Link color="inherit" href="https://tensorlake.ai/">
        Tensorlake
      </Link>{" "}
      {new Date().getFullYear()}
      {"."}
    </Typography>
  );
}

interface AppBarProps extends MuiAppBarProps {
  open?: boolean;
}

const AppBar = styled(MuiAppBar, {
  shouldForwardProp: (prop) => prop !== "open",
})<AppBarProps>(({ theme, open }) => ({
  transition: theme.transitions.create(["width", "margin"], {
    easing: theme.transitions.easing.sharp,
    duration: theme.transitions.duration.leavingScreen,
  }),
}));

export default function Dashboard() {
  const { namespace, namespaces } = useLoaderData() as {
    namespace: string;
    namespaces: string[];
  };
  const [anchorEl, setAnchorEl] = React.useState<null | HTMLElement>(null);

  const handleMenu = (event: React.MouseEvent<HTMLElement>) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  return (
    <ThemeProvider theme={theme}>
      <Box sx={{ display: "flex" }}>
        <CssBaseline />
        <AppBar position="absolute" sx={{ backgroundColor: "white" }}>
          <Toolbar
            sx={{
              pr: "24px", // keep right padding when drawer closed
            }}
          >
            <Stack
              direction={"row"}
              display={"flex"}
              alignItems={"center"}
              justifyContent={"flex-start"}
              spacing={2}
              flexGrow={1}
            >
              <img src="/ui/logo.svg" alt="logo" />
              <a
                href={"/ui"}
                style={{ textDecoration: "none", color: "white" }}
              >
                <Typography
                  component="h1"
                  variant="h6"
                  color="#060D3F"
                  noWrap
                  sx={{ flexGrow: 1 }}
                >
                  Indexify
                </Typography>
              </a>
            </Stack>

            <Button
              size="large"
              aria-label="account of current user"
              aria-controls="menu-appbar"
              aria-haspopup="true"
              onClick={handleMenu}
              startIcon={
                <DataObjectIcon sx={{ color: stringToColor(namespace) }} />
              }
            >
              {namespace}
            </Button>

            <Menu
              id="menu-appbar"
              anchorEl={anchorEl}
              anchorOrigin={{
                vertical: "top",
                horizontal: "right",
              }}
              keepMounted
              transformOrigin={{
                vertical: "top",
                horizontal: "right",
              }}
              open={Boolean(anchorEl)}
              onClose={handleClose}
            >
              <Typography mx={2} my={1} variant="h4">
                Namespaces ({namespaces.length})
              </Typography>
              {namespaces.map((name) => {
                return (
                  <a
                    style={{ textDecoration: "none" }}
                    key={name}
                    href={`/ui/${name}`}
                  >
                    <MenuItem onClick={handleClose}>
                      <CircleIcon
                        sx={{
                          width: "15px",
                          color: stringToColor(name),
                          mr: 1,
                        }}
                      />
                      {name}
                    </MenuItem>
                  </a>
                );
              })}
            </Menu>
          </Toolbar>
        </AppBar>

        <Box
          component="main"
          sx={{
            backgroundColor: (theme) =>
              theme.palette.mode === "light"
                ? theme.palette.grey[100]
                : theme.palette.grey[900],
            flexGrow: 1,
            height: "100vh",
            overflow: "auto",
          }}
        >
          <Toolbar />
          <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
            <div id="detail">
              <Outlet />
            </div>
            <Copyright sx={{ pt: 4 }} />
          </Container>
        </Box>
      </Box>
    </ThemeProvider>
  );
}
