import React from "react";
import ReactDOM from "react-dom/client";
import "./index.css";

import {
  createBrowserRouter,
  Navigate,
  RouterProvider,
  useParams
} from "react-router-dom";

import Root from "./routes/root";
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
    return null;
  } else if (namespace === "namespace") {
    return <Navigate to={`/${namespace}/compute-graphs`} replace />;
  } else {
    return <Navigate to={`/${namespace}/compute-graphs`} replace />;
  }
}

async function rootLoader() {
  const namespacesData = await NamespacesPageLoader();
  return namespacesData;
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