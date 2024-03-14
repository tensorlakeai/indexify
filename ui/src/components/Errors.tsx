import Alert from "@mui/material/Alert";

const Errors = ({ errors }: { errors: string[] }) => {
  return (
    <div>
      {errors.map((e, i) => (
        <Alert key={`error-${i}`} severity="error">{e}</Alert>
      ))}
    </div>
  );
};

export default Errors;
