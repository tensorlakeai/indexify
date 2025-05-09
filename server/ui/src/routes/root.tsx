import { type ReactElement } from 'react'
import { ThemeProvider } from '@mui/material/styles'
import {
  Box,
  CssBaseline,
  Toolbar,
  Typography,
  Drawer,
  List,
  ListItemButton,
  ListItemText,
  Divider,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  SelectChangeEvent,
} from '@mui/material'
import {
  LoaderFunctionArgs,
  Outlet,
  redirect,
  useLocation,
  Link,
  useParams,
} from 'react-router-dom'
import { Cpu, Setting4 } from 'iconsax-react'
import { getIndexifyServiceURL } from '../utils/helpers'
import theme from '../theme'

import Footer from '../components/Footer'
import VersionDisplay from '../components/VersionDisplay'
import { useLoaderData } from 'react-router-dom'

const DRAWER_WIDTH = 240
const SERVICE_URL = getIndexifyServiceURL()

interface NavItem {
  path: string
  icon: ReactElement
  label: string
}

interface RootLoaderData {
  namespaces: { name: string; created_at: number }[]
  namespace: string
}

export async function loader({ params }: LoaderFunctionArgs) {
  const namespace = params.namespace || 'default';
  return redirect(`/${namespace}/compute-graphs`);
}

function Dashboard() {
  const location = useLocation();
  const navigate = useNavigate();
  const { namespace = 'default' } = useParams<{ namespace: string }>();
  const { namespaces } = useLoaderData() as RootLoaderData;

  const handleNamespaceChange = (event: SelectChangeEvent) => {
    const newNamespace = event.target.value
    const newPath = location.pathname.replace(/^\/[^/]+/, `/${newNamespace}`)
    window.location.href = window.location.origin + '/ui' + newPath
  }

  const navItems: NavItem[] = [
    {
      path: `${namespace}/compute-graphs`,
      icon: <Cpu size="20" className="drawer-logo" variant="Outline" />,
      label: 'Compute Graphs',
    },
    {
      path: '/executors',
      icon: <Setting4 size="20" className="drawer-logo" variant="Outline" />,
      label: 'Executors',
    },
  ]

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
        <VersionDisplay
          owner="tensorlakeai"
          repo="indexify"
          variant="announcement"
          serviceUrl={SERVICE_URL}
          drawerWidth={DRAWER_WIDTH}
        />

        <Box sx={{ display: 'flex', flex: 1 }}>
          <Drawer
            variant="permanent"
            sx={{
              width: DRAWER_WIDTH,
              flexShrink: 0,
              '& .MuiDrawer-paper': {
                width: DRAWER_WIDTH,
                boxSizing: 'border-box',
              },
            }}
          >
            <Toolbar>
              <Box display="flex" alignItems="center" gap={2}>
                <img src="/ui/logo.svg" alt="logo" />
                <Link to="/ui" style={{ textDecoration: 'none' }}>
                  <Typography
                    component="h1"
                    variant="h6"
                    color="#060D3F"
                    noWrap
                  >
                    Indexify
                  </Typography>
                </Link>
              </Box>
            </Toolbar>

            <Box
              sx={{
                display: 'flex',
                flexDirection: 'column',
                height: '100%',
                overflow: 'auto',
              }}
            >
              <Box sx={{ p: 1 }}>
                <FormControl fullWidth size="small">
                  <InputLabel id="namespace-select-label">Namespace</InputLabel>
                  <Select
                    labelId="namespace-select-label"
                    id="namespace-select"
                    value={namespace}
                    label="Namespace"
                    onChange={handleNamespaceChange}
                  >
                    {namespaces.map((ns) => (
                      <MenuItem key={ns.name} value={ns.name}>
                        {ns.name}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Box>
              <List sx={{ flexGrow: 1 }}>
                {navItems.map(({ path, icon, label }) => (
                  <ListItemButton
                    key={path}
                    to={path}
                    component={Link}
                    selected={location.pathname.startsWith(path)}
                    className={
                      location.pathname.startsWith(path)
                        ? 'selected-navbar-items navbar-items'
                        : 'navbar-items'
                    }
                  >
                    {icon}
                    <ListItemText primary={label} />
                  </ListItemButton>
                ))}
              </List>

              <Box sx={{ mt: 'auto', pb: 1 }}>
                <VersionDisplay
                  owner="tensorlakeai"
                  repo="indexify"
                  variant="sidebar"
                  serviceUrl={SERVICE_URL}
                  drawerWidth={DRAWER_WIDTH}
                />
              </Box>
            </Box>
          </Drawer>

          <Box
            component="main"
            display="flex"
            flexDirection="column"
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
  )
}

export default Dashboard
