import React from "react";
import ReactDOM from "react-dom/client";
import "./index.css";

import {
  createBrowserRouter,
  Navigate,
  RouterProvider,
  useParams
} from "react-router-dom";

import Root, { loader as RootLoader } from "./routes/root";
import { ErrorPage } from "./error-page";
import {
  ComputeGraphsPageLoader,
  ExecutorsPageLoader,
  IndividualComputeGraphPageLoader,
  IndividualInvocationPageLoader,
  NamespacesPageLoader,
} from "./utils/loaders";
import {
  ComputeGraphsPage,
  NamespacesPage,
  IndividualComputeGraphPage,
  IndividualInvocationPage,
  ExecutorsPage,
} from "./routes/Namespace";

function RedirectToComputeGraphs() {
  const { namespace } = useParams();
  
  if (namespace === "namespaces") {
    // Don't redirect if the param is "namespaces"
    return null;
  } else if (namespace === "namespace") {
    // Redirect to extractors if the param is "namespace"
    return <Navigate to={`/${namespace}/extractors`} replace />;
  } else {
    // Original behavior for other cases
    return <Navigate to={`/${namespace}/extractors`} replace />;
  }
}

const router = createBrowserRouter(
  [
    {
      path: "/",
      element: <Root />,
      errorElement: <ErrorPage />,
      loader: RootLoader,
      children: [
        {
          path: "/:namespace",
          element: <RedirectToComputeGraphs />
        },
        {
          path: "/namespaces",
          element: <NamespacesPage />,
          loader: NamespacesPageLoader,
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
