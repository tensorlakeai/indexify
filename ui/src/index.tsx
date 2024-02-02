import React from "react";
import ReactDOM from "react-dom/client";
import "./index.css";
import App from "./App";

import { createBrowserRouter, RouterProvider } from "react-router-dom";

import Dashboard from "./routes/Dashboard";

const router = createBrowserRouter([
  {
    path: "/*",
    element: <Dashboard />,
    // loader: rootLoader,
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
