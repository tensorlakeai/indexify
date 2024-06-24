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
import ExtractionPolicyPage, {
  loader as ExtractionPolicyLoader
} from "./routes/Namespace/extractionPolicy";
import ContentPage, {
  loader as ContentLoader
} from "./routes/Namespace/content-single";
import ExtractorsPage, {
  loader as ExtractorsLoader
} from "./routes/Namespace/extractors";
import ExtractionGraphsPage, {
  loader as ExtractionGraphLoader
} from "./routes/Namespace/extractionGraphs";
import SearchIndexPage, {
  loader as SearchIndexLoader
} from "./routes/Namespace/extractionPolicy";
import IndexesPage, {
  loader as IndexesLoader
} from "./routes/Namespace/indexes";
import SqlTablesPage, {
  loader as SqlTablesLoader
} from "./routes/Namespace/sqlTables";
import ContentsPage, {
  loader as ContentsLoader
} from "./routes/Namespace/content";

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
          loader: ExtractionPolicyLoader,
          errorElement: <ErrorPage />
        },
        {
          path: "/:namespace/indexes/:indexName",
          element: <SearchIndexPage />,
          loader: SearchIndexLoader,
          errorElement: <ErrorPage />
        },
        {
          path: "/:namespace/content/:contentId",
          element: <ContentPage />,
          loader: ContentLoader,
          errorElement: <ErrorPage />
        },
        {
          path: "/:namespace/content",
          element: <ContentsPage />,
          loader: ContentsLoader,
          errorElement: <ErrorPage />
        },
        {
          path: "/:namespace/extractors",
          element: <ExtractorsPage />,
          loader: ExtractorsLoader,
          errorElement: <ErrorPage />
        },
        {
          path: "/:namespace/extraction-graphs",
          element: <ExtractionGraphsPage />,
          loader: ExtractionGraphLoader,
          errorElement: <ErrorPage />
        },
        {
          path: "/:namespace/indexes",
          element: <IndexesPage />,
          loader: IndexesLoader,
          errorElement: <ErrorPage />
        },
        {
          path: "/:namespace/sql-tables",
          element: <SqlTablesPage />,
          loader: SqlTablesLoader,
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
