import { createTheme } from "@mui/material/styles";

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
            fontFamily: "poppins",
            "&.Mui-disabled": {
              backgroundColor: "rgba(6, 13, 63, 0.1)",
              borderColor: "#676767",
            },
          }),
        outlined: {
          padding: "10px 24px",
          borderRadius: "8px",
          color: "primary",
          border: "1px solid #3296FE",
        },
        contained: {
          padding: "10px 16px",
          borderRadius: "8px",
          border: "1px solid #3296FE",
          backgroundColor: "#3296FE",
          ":hover": {
            backgroundColor: "#1080f4",
          }
        },
        text: {
          padding: "10px 12px",
        },
      },
    },
  },

  typography: {
    fontFamily: "poppins",
    h1: {
      fontFamily: "poppins",
      fontSize: "48px",
      lineHeight: "normal",
      fontStyle: "normal",
      fontWeight: "600",
    },
    h2: {
      fontFamily: "poppins",
      fontSize: "32px",
      lineHeight: "normal",
      fontStyle: "normal",
      fontWeight: "600",
    },
    h3: {
      fontFamily: "poppins",
      fontSize: "24px",
      fontStyle: "normal",
      fontWeight: "700",
      lineHeight: "normal",
    },
    h4: {
      fontFamily: "poppins",
      fontSize: "20px",
      fontStyle: "normal",
      fontWeight: "500",
      lineHeight: "normal",
    },
    label: {
      fontFamily: "poppins",
      fontSize: "16px",
      fontStyle: "normal",
      fontWeight: "500",
      lineHeight: "22px",
      letterSpacing: 0.32,
    },
    labelSmall: {
      fontFamily: "poppins",
      fontSize: "13px",
      fontStyle: "normal",
      fontWeight: "600",
      lineHeight: "22px",
      letterSpacing: 0.32,
    },
    body1: {
      fontFamily: "poppins",
      fontSize: "16px",
      fontStyle: "normal",
      fontWeight: "400",
      lineHeight: "22px",
      letterSpacing: 0.32,
    },
    body2: {
      fontFamily: "poppins",
      fontSize: "16px",
      fontStyle: "normal",
      fontWeight: "400",
      lineHeight: "normal",
    },
  },
});

export default theme;
