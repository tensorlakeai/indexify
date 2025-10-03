import React from 'react'
import ReactDOM from 'react-dom/client'
import './index.css'

import {
  createBrowserRouter,
  LoaderFunctionArgs,
  Navigate,
  RouterProvider,
  useParams,
} from 'react-router-dom'

import { IndexifyClient } from 'getindexify'
import { ErrorPage } from './error-page'
import {
  ApplicationDetailsPage,
  ApplicationsListPage,
  GraphRequestDetailsPage,
} from './routes/Namespace'
import ExecutorsPage from './routes/Namespace/ExecutorsPage'
import Root from './routes/root'
import { getIndexifyServiceURL } from './utils/helpers'
import {
  ApplicationsDetailsPageLoader,
  ApplicationsListPageLoader,
  ExecutorsPageLoader,
  GraphRequestDetailsPageLoader,
} from './utils/loaders'

function RedirectToComputeGraphs() {
  const { namespace } = useParams<{ namespace: string }>()

  if (namespace === 'namespaces') return null

  const currentNamespace = namespace || 'default'
  return <Navigate to={`/${currentNamespace}/applications`} replace />
}

function RootRedirect() {
  const { namespace = 'default' } = useParams<{ namespace: string }>()
  return <Navigate to={`/${namespace}/applications`} replace />
}

async function rootLoader({ params }: LoaderFunctionArgs) {
  try {
    const serviceUrl = getIndexifyServiceURL()
    const response = await IndexifyClient.namespaces({ serviceUrl })

    return {
      namespaces: response,
      namespace: params.namespace || 'default',
    }
  } catch (error) {
    console.error('Failed to load namespaces:', error)
    throw new Error('Failed to load namespaces. Please try again later.')
  }
}

const router = createBrowserRouter(
  [
    {
      path: '/',
      element: <Root />,
      errorElement: <ErrorPage />,
      loader: rootLoader,
      children: [
        {
          index: true,
          element: <RootRedirect />,
          errorElement: <ErrorPage />,
        },
        {
          path: '/:namespace',
          element: <RedirectToComputeGraphs />,
          errorElement: <ErrorPage />,
        },
        {
          path: '/:namespace/applications',
          element: <ApplicationsListPage />,
          loader: ApplicationsListPageLoader,
          errorElement: <ErrorPage />,
        },
        {
          path: '/:namespace/applications/:application',
          element: <ApplicationDetailsPage />,
          loader: ApplicationsDetailsPageLoader,
          errorElement: <ErrorPage />,
        },
        {
          path: '/:namespace/applications/:application/requests/:request-id',
          element: <GraphRequestDetailsPage />,
          loader: GraphRequestDetailsPageLoader,
          errorElement: <ErrorPage />,
        },
        {
          path: '/executors',
          element: <ExecutorsPage />,
          loader: ExecutorsPageLoader,
          errorElement: <ErrorPage />,
        },
      ],
    },
  ],
  { basename: '/ui' }
)

const root = ReactDOM.createRoot(document.getElementById('root') as HTMLElement)

root.render(
  <React.StrictMode>
    <RouterProvider router={router} />
  </React.StrictMode>
)
