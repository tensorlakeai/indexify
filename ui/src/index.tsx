import React from "react";
import ReactDOM from "react-dom/client";
import "./index.css";

import { createBrowserRouter, RouterProvider } from "react-router-dom";

import Dashboard from "./routes/root";
import { ErrorPage } from "./error-page";

const router = createBrowserRouter([
  {
    path: "/*",
    element: <Dashboard />,
    errorElement: <ErrorPage />,
    // children: [
    //   {
    //     path: "test",
    //     element: <Team />,
    //     loader: teamLoader,
    //   },
    // ],
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
