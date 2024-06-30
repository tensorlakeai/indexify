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
  ExtractionPolicyPageLoader,
  SearchIndexPageLoader,
  IndividualContentPageLoader,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  ContentsPageLoader,
  ExtractorsPageLoader,
  ExtractionGraphsPageLoader,
  IndexesPageLoader,
  SqlTablesPageLoader
} from "./utils/loaders";
import {
  ExtractionPolicyPage,
  SearchIndexPage,
  IndividualContentPage,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  ContentsPage,
  ExtractorsPage,
  ExtractionGraphsPage,
  IndexesPage,
  SqlTablesPage
} from "./routes/Namespace";

function RedirectToExtractors() {
  const { namespace } = useParams();
  return <Navigate to={`/${namespace}/extractors`} replace />;
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
          element: <RedirectToExtractors />
        },
        {
          path: "/:namespace/extraction-policies/:graphname/:policyname",
          element: <ExtractionPolicyPage />,
          loader: ExtractionPolicyPageLoader,
          errorElement: <ErrorPage />
        },
        {
          path: "/:namespace/indexes/:indexName",
          element: <SearchIndexPage />,
          loader: SearchIndexPageLoader,
          errorElement: <ErrorPage />
        },
        {
          path: "/:namespace/content/:contentId",
          element: <IndividualContentPage />,
          loader: IndividualContentPageLoader,
          errorElement: <ErrorPage />
        },
        // {
        //   path: "/:namespace/content",
        //   element: <ContentsPage />,
        //   loader: ContentsPageLoader,
        //   errorElement: <ErrorPage />
        // },
        {
          path: "/:namespace/extractors",
          element: <ExtractorsPage />,
          loader: ExtractorsPageLoader,
          errorElement: <ErrorPage />
        },
        {
          path: "/:namespace/extraction-graphs",
          element: <ExtractionGraphsPage />,
          loader: ExtractionGraphsPageLoader,
          errorElement: <ErrorPage />
        },
        {
          path: "/:namespace/indexes",
          element: <IndexesPage />,
          loader: IndexesPageLoader,
          errorElement: <ErrorPage />
        },
        {
          path: "/:namespace/sql-tables",
          element: <SqlTablesPage />,
          loader: SqlTablesPageLoader,
          errorElement: <ErrorPage />
        }
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
