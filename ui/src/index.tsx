import React from "react";
import ReactDOM from "react-dom/client";
import "./index.css";

import { createBrowserRouter, RouterProvider } from "react-router-dom";

import Root from "./routes/root";
import { ErrorPage } from "./error-page";
import Repositories, {
  loader as RepositoriesLoader,
} from "./routes/repositories";
import Extractors, { loader as ExtractorsLoader } from "./routes/extractors";
import Repository, { loader as RepositoryLoader } from "./routes/repository";
import HomePage from "./routes/home";

const router = createBrowserRouter([
  {
    path: "/",
    element: <Root />,
    errorElement: <ErrorPage />,
    children: [
      {
        path: "/",
        element: <HomePage />,
        errorElement: <ErrorPage />,
      },
      {
        path: "/repositories",
        element: <Repositories />,
        loader: RepositoriesLoader,
        errorElement: <ErrorPage />,
      },
      {
        path: "/repositories/:repositoryname",
        element: <Repository />,
        loader: RepositoryLoader,
        errorElement: <ErrorPage />,
      },
      {
        path: "/extractors",
        element: <Extractors />,
        loader: ExtractorsLoader,
        errorElement: <ErrorPage />,
      },
    ],
  },
]);

const root = ReactDOM.createRoot(
  document.getElementById("root") as HTMLElement
);
root.render(
  <React.StrictMode>
    <RouterProvider router={router} />
  </React.StrictMode>
);
