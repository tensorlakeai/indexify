import { IExtractedMetadata } from "getindexify";

export const stringToColor = (str: string) => {
  let hash = 0;
  str.split("").forEach((char) => {
    hash = char.charCodeAt(0) + ((hash << 5) - hash);
  });
  let color = "#";
  for (let i = 0; i < 3; i++) {
    const value = (hash >> (i * 8)) & 0xff;
    color += value.toString(16).padStart(2, "0");
  }
  return color;
};

export const groupMetadataByExtractor = (
  metadataArray: IExtractedMetadata[]
): Record<string, IExtractedMetadata[]> => {
  return metadataArray.reduce((accumulator, currentItem) => {
    // Use the extractor_name as the key
    const key = currentItem.extractor_name;

    // If the key doesn't exist yet, initialize it
    if (!accumulator[key]) {
      accumulator[key] = [];
    }

    // Add the current item to the appropriate group
    accumulator[key].push(currentItem);

    return accumulator;
  }, {} as Record<string, IExtractedMetadata[]>);
};

export const getIndexifyServiceURL = (): string => {
  if (process.env.NODE_ENV === "development") {
    return "http://localhost:8900";
  }
  return window.location.origin;
};

export const formatBytes = (bytes: number, decimals: number = 2): string => {
  if (!+bytes) return "0 Bytes";

  const k = 1000;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];

  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`;
};
