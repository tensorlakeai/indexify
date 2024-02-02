import React from "react";
import ReactDOM from "react-dom/client";
import "./index.css";

import { createBrowserRouter, RouterProvider } from "react-router-dom";

import Root from "./routes/root";
import { ErrorPage } from "./error-page";
import Repositories from "./routes/repositories";
import Extractors from "./routes/extractors";
import Repository, { loader as repositoryLoader } from "./routes/repository";

const router = createBrowserRouter([
  {
    path: "/",
    element: <Root />,
    errorElement: <ErrorPage />,
    children: [
      {
        path: "/repositories",
        element: <Repositories />,
      },
      {
        path: "/repositories/:repositoryname",
        element: <Repository />,
        loader: repositoryLoader,
      },
      {
        path: "/extractors",
        element: <Extractors />,
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
