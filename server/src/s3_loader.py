from typing import Generic,


# CUSTOMER CODE

class File(BaseModel):
    content: bytes
    size: int
    name: str
    checksum: str

@tensorlake_function(name="s3_loader")
def s3_loader(
    bucket: str,
    key: str,
    region: str = "us-east-1",
) -> Generator[File, str]:
    # read the file from s3
    yield File(content=b"", size=0, name="", checksum="")



#### HOST EXECUTOR 


outputs = s3_loader(bucket="mybucket", key="mykey")

for output in outputs:
    # upload to indexify server 

