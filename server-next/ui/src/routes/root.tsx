import { ThemeProvider } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';
import Box from '@mui/material/Box';
import Toolbar from '@mui/material/Toolbar';
import Typography from '@mui/material/Typography';
import {
  LoaderFunctionArgs,
  Outlet,
  redirect,
  useLoaderData,
  useLocation,
} from 'react-router-dom';
import theme from '../theme';
import { Stack } from '@mui/system';
import { IndexifyClient } from 'getindexify';
import { getIndexifyServiceURL } from '../utils/helpers';
import Footer from '../components/Footer';
import {
  Divider,
  Drawer,
  List,
  ListItemButton,
  ListItemText,
} from '@mui/material';
import { Link } from 'react-router-dom';
import { Cpu, TableDocument } from 'iconsax-react';
import VersionDisplay from '../components/VersionDisplay';

const indexifyServiceURL = getIndexifyServiceURL();

export async function loader({ params }: LoaderFunctionArgs) {
  const namespaces = (
    await IndexifyClient.namespaces({
      serviceUrl: indexifyServiceURL,
    })
  ).map((repo: any) => repo.name);

  if (!params.namespace || !namespaces.includes(params.namespace)) {
    if (params.namespace !== 'default') {
      return redirect(`/${namespaces[0] ?? 'default'}/compute-graphs`);
    }
  }
  return { namespaces, namespace: params.namespace };
}

const drawerWidth = 240;

export default function Dashboard() {
  const { namespace } = useLoaderData() as {
    namespace: string;
    namespaces: string[];
  };
  const location = useLocation();

  return (
    <ThemeProvider theme={theme}>
      <Box
        sx={{
          display: 'flex',
          flexDirection: 'column',
          height: '100vh',
          backgroundColor: (theme) =>
            theme.palette.mode === 'light'
              ? '#F7F9FC'
              : theme.palette.grey[900],
        }}
      >
        <CssBaseline />
        <VersionDisplay owner="tensorlakeai" repo="indexify" variant="announcement" serviceUrl={indexifyServiceURL} drawerWidth={240} />
        <Box sx={{ display: 'flex', flex: 1 }}>
          <Drawer
            variant="permanent"
            sx={{
              width: drawerWidth,
              flexShrink: 0,
              [`& .MuiDrawer-paper`]: {
                width: drawerWidth,
                boxSizing: 'border-box',
              },
            }}
          >
            <Toolbar>
              <Stack
                direction={'row'}
                display={'flex'}
                alignItems={'center'}
                justifyContent={'flex-start'}
                spacing={2}
              >
                <img src="/ui/logo.svg" alt="logo" />
                <a
                  href={'/ui'}
                  style={{ textDecoration: 'none', color: 'white' }}
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
            </Toolbar>
            <Box
              sx={{
                display: 'flex',
                flexDirection: 'column',
                height: '100%',
                overflow: 'auto',
              }}
            >
              <List sx={{ flexGrow: 1 }}>
                <ListItemButton
                  to={`/namespaces`}
                  component={Link}
                  selected={location.pathname.startsWith(`/namespaces`)}
                  className={location.pathname.startsWith(`/namespaces`) ? "selected-navbar-items navbar-items" : "navbar-items"}
                >
                  <TableDocument size="20" className="drawer-logo" variant="Outline" />
                  <ListItemText primary={'Namespaces'} />
                </ListItemButton>
                <ListItemButton
                  to={`/${namespace}/compute-graphs`}
                  component={Link}
                  selected={location.pathname.startsWith(`/${namespace}/compute-graphs`)}
                  className={location.pathname.startsWith(`/${namespace}/compute-graphs`) ? "selected-navbar-items navbar-items" : "navbar-items"}
                >
                  <Cpu size="20" className="drawer-logo" variant="Outline" />
                  <ListItemText primary={'Compute Graphs'} />
                </ListItemButton>
              </List>
              <Box sx={{ mt: 'auto', pb: 1 }}>
                <VersionDisplay owner="tensorlakeai" repo="indexify" variant="sidebar" serviceUrl={indexifyServiceURL} drawerWidth={240} />
              </Box>
            </Box>
          </Drawer>
          {/* page content */}
          <Box
            component="main"
            display="flex"
            flexDirection={'column'}
            sx={{
              flexGrow: 1,
              height: '100vh',
              overflow: 'auto',
              padding: 2,
            }}
          >
            <Box id="detail" p={2} flexGrow={1}>
              <Outlet />
            </Box>
            <Divider />
            <Footer />
          </Box>
        </Box>
      </Box>
    </ThemeProvider>
  );
}
