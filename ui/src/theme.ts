import { createTheme } from "@mui/material/styles";

// const roboto = Roboto({
//   weight: ["300", "400", "500", "700"],
//   subsets: ["latin"],
//   display: "swap",
// });

// const rubik = Rubik({
//   weight: ["600", "700"],
//   subsets: ["latin"],
//   display: "swap",
// });

// const outfit = Outfit({
//   weight: ["400", "600", "700"],
//   subsets: ["latin"],
//   display: "swap",
// });

declare module "@mui/material/styles" {
  interface TypographyVariants {
    labelSmall: React.CSSProperties;
    label: React.CSSProperties;
    menuItem: React.CSSProperties;
  }

  // allow configuration using `createTheme`
  interface TypographyVariantsOptions {
    labelSmall?: React.CSSProperties;
    label?: React.CSSProperties;
    menuItem?: React.CSSProperties;
  }
}

// Update the Typography's variant prop options
declare module "@mui/material/Typography" {
  interface TypographyPropsVariantOverrides {
    labelSmall: true
    label: true;
    menuItem: true;
  }
}

/* TO Define
  Breakpoints
*/

const theme = createTheme({
  palette: {
    mode: "light",
    text: {
      primary: "#060D3F",
    },
    primary: {
      main: "#060D3F",
    },
  },
  components: {
    MuiContainer: {
      defaultProps: {
        maxWidth: "lg",
      },
    },
    MuiChip: {
      styleOverrides: {
        root: {
          height: "32px",
          borderRadius: "8px",
        },
      },
    },
    MuiSelect: {
      styleOverrides: {
        root: {
          "& .MuiOutlinedInput-notchedOutline": {
            borderColor: "gray",
          },
        },
        select: {
          backgroundColor: "white",
        },
      },
    },
    MuiTextField: {
      styleOverrides: {
        root: {
          "& .MuiInputBase-root": { backgroundColor: "white" }, // For outlined, filled variants
          "& .MuiOutlinedInput-notchedOutline": { borderColor: "gray" }, // Optional: Customize border color
        },
      },
    },
    MuiButton: {
      styleOverrides: {
        root: ({ theme }) =>
          theme.unstable_sx({
            boxShadow: "none",
            lineHeight: "20px",
            fontSize: "14px",
            textTransform: "none",
            letterSpacing: "0.1px",
            fontFamily: "roboto",
            "&.Mui-disabled": {
              backgroundColor: "rgba(6, 13, 63, 0.1)",
              borderColor: "#676767",
            },
          }),
        outlined: {
          padding: "10px 24px",
          borderRadius: "100px",
          color: "primary",
          border: "1px solid #79747E",
        },
        contained: {
          padding: "10px 24px",
          borderRadius: "100px",
          border: "1px solid #060D3F",
        },
        text: {
          padding: "10px 12px",
        },
      },
    },
  },

  typography: {
    fontFamily: "roboto",
    h1: {
      fontFamily: "outfit",
      fontSize: "48px",
      lineHeight: "normal",
      fontStyle: "normal",
      fontWeight: "600",
    },
    h2: {
      fontFamily: "outfit",
      fontSize: "32px",
      lineHeight: "normal",
      fontStyle: "normal",
      fontWeight: "600",
    },
    h3: {
      fontFamily: "roboto",
      fontSize: "24px",
      fontStyle: "normal",
      fontWeight: "700",
      lineHeight: "normal",
    },
    h4: {
      fontFamily: "outfit",
      fontSize: "20px",
      fontStyle: "normal",
      fontWeight: "500",
      lineHeight: "normal",
    },
    label: {
      fontFamily: "outfit",
      fontSize: "16px",
      fontStyle: "normal",
      fontWeight: "500",
      lineHeight: "22px",
      letterSpacing: 0.32,
    },
    labelSmall: {
      fontFamily: "outfit",
      fontSize: "13px",
      fontStyle: "normal",
      fontWeight: "600",
      lineHeight: "22px",
      letterSpacing: 0.32,
    },
    body1: {
      fontFamily: "outfit",
      fontSize: "16px",
      fontStyle: "normal",
      fontWeight: "400",
      lineHeight: "22px",
      letterSpacing: 0.32,
    },
    body2: {
      fontFamily: "outfit",
      fontSize: "16px",
      fontStyle: "normal",
      fontWeight: "400",
      lineHeight: "normal",
    },
  },
});

export default theme;
