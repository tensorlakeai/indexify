import React from "react";
import ReactDOM from "react-dom/client";
import "./index.css";

import { createBrowserRouter, RouterProvider } from "react-router-dom";

import Root, { loader as RootLoader } from "./routes/root";
import { ErrorPage } from "./error-page";
import Namespace, { loader as NamespaceLoader } from "./routes/Namespace";
import ExtractorBindingPage, {
  loader as ExtractorBindingLoader,
} from "./routes/Namespace/bindings";
import ContentPage, {
  loader as ContentLoader,
} from "./routes/Namespace/content";

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
          element: <Namespace />,
          loader: NamespaceLoader,
          errorElement: <ErrorPage />,
        },
        {
          path: "/:namespace/bindings/:bindingname",
          element: <ExtractorBindingPage />,
          loader: ExtractorBindingLoader,
          errorElement: <ErrorPage />,
        },
        {
          path: "/:namespace/content/:contentId",
          element: <ContentPage />,
          loader: ContentLoader,
          errorElement: <ErrorPage />,
        },
      ],
    },
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
