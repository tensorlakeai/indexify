import React from "react";
import ReactDOM from "react-dom/client";
import "./index.css";

import {
  createBrowserRouter,
  Navigate,
  RouterProvider,
  useParams,
  LoaderFunctionArgs
} from "react-router-dom";

import Root from "./routes/root";
import { ErrorPage } from "./error-page";
import {
  ComputeGraphsPageLoader,
  ExecutorsPageLoader,
  IndividualComputeGraphPageLoader,
  IndividualInvocationPageLoader,
} from "./utils/loaders";
import {
  ComputeGraphsPage,
  IndividualComputeGraphPage,
  IndividualInvocationPage,
  ExecutorsPage,
} from "./routes/Namespace";
import { IndexifyClient } from "getindexify";
import { getIndexifyServiceURL } from "./utils/helpers";

function RedirectToComputeGraphs() {
  const { namespace } = useParams<{ namespace: string }>();
  
  if (namespace === "namespaces") return null;
  
  const currentNamespace = namespace || 'default';
  return <Navigate to={`/${currentNamespace}/compute-graphs`} replace />;
}

async function rootLoader({ params }: LoaderFunctionArgs) {
  try {
    const serviceUrl = getIndexifyServiceURL();
    const response = await IndexifyClient.namespaces({ serviceUrl });
    
    return { 
      namespaces: response, 
      namespace: params.namespace || 'default'
    };
  } catch (error) {
    console.error('Failed to load namespaces:', error);
    throw new Error('Failed to load namespaces. Please try again later.');
  }
}

const router = createBrowserRouter(
  [
    {
      path: "/",
      element: <Root />,
      errorElement: <ErrorPage />,
      loader: rootLoader,
      children: [
        {
          path: "/:namespace",
          element: <RedirectToComputeGraphs />,
          errorElement: <ErrorPage />
        },
        {
          path: "/:namespace/compute-graphs",
          element: <ComputeGraphsPage />,
          loader: ComputeGraphsPageLoader,
          errorElement: <ErrorPage />
        },
        {
          path: "/:namespace/compute-graphs/:compute-graph",
          element: <IndividualComputeGraphPage />,
          loader: IndividualComputeGraphPageLoader,
          errorElement: <ErrorPage />
        },
        {
          path: "/:namespace/compute-graphs/:compute-graph/invocations/:invocation-id",
          element: <IndividualInvocationPage />,
          loader: IndividualInvocationPageLoader,
          errorElement: <ErrorPage />
        },
        {
          path: "/executors",
          element: <ExecutorsPage />,
          loader: ExecutorsPageLoader,
          errorElement: <ErrorPage />
        },
      ]
    }
  ],
  { basename: "/ui" }
);

const root = ReactDOM.createRoot(
  document.getElementById("root") as HTMLElement
);

root.render(
  <React.StrictMode>
    <RouterProvider router={router} />
  </React.StrictMode>
);
