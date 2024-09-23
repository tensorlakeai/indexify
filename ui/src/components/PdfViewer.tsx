// import { useState } from "react";
// import { Document, Page, pdfjs } from "react-pdf";
// import "react-pdf/dist/esm/Page/AnnotationLayer.css";
// import "react-pdf/dist/esm/Page/TextLayer.css";
// import { Box, Button, Typography } from "@mui/material";

// pdfjs.GlobalWorkerOptions.workerSrc = `//cdnjs.cloudflare.com/ajax/libs/pdf.js/${pdfjs.version}/pdf.worker.min.js`;

// interface PdfDisplayProps {
//   url: string;
// }

// const PdfDisplay: React.FC<PdfDisplayProps> = ({ url }) => {
//   const [numPages, setNumPages] = useState<number>(0);
//   const [pageNumber, setPageNumber] = useState<number>(1);

//   function onDocumentLoadSuccess({ numPages }: { numPages: number }): void {
//     setNumPages(numPages);
//     setPageNumber(1);
//   }

//   return (
//     <div>
//       <Box sx={{ maxWidth: "500px" }}>
//         {numPages > 1 && (
//           <Box
//             display="flex"
//             justifyContent="center"
//             alignItems="center"
//             gap={2}
//             mb={2}
//           >
//             <Button
//               variant="contained"
//               color="primary"
//               disabled={pageNumber <= 1}
//               onClick={() => setPageNumber((prev) => prev - 1)}
//             >
//               Previous
//             </Button>
//             <Typography sx={{ userSelect: "none" }}>
//               Page {pageNumber} of {numPages}
//             </Typography>
//             <Button
//               variant="contained"
//               color="primary"
//               disabled={pageNumber >= numPages}
//               onClick={() => setPageNumber((prev) => prev + 1)}
//             >
//               Next
//             </Button>
//           </Box>
//         )}
//         <Document file={url} onLoadSuccess={onDocumentLoadSuccess}>
//           <Page pageNumber={pageNumber} />
//         </Document>
//       </Box>
//     </div>
//   );
// };

// export default PdfDisplay;
export {}
