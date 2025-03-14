use std::pin::Pin;

use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{Request, Response, Status};
use tracing::info;

use super::executor_api_pb::{
    executor_api_server::ExecutorApi,
    DesiredExecutorState,
    GetDesiredExecutorStatesRequest,
    ReportExecutorStateRequest,
    ReportExecutorStateResponse,
};

#[derive(Default)]
pub struct ExecutorAPIService;

#[tonic::async_trait]
impl ExecutorApi for ExecutorAPIService {
    #[allow(non_camel_case_types)] // The autogenerated code in the trait uses snake_case types in some cases
    type get_desired_executor_statesStream =
        Pin<Box<dyn Stream<Item = Result<DesiredExecutorState, Status>> + Send>>;

    async fn report_executor_state(
        &self,
        request: Request<ReportExecutorStateRequest>,
    ) -> Result<Response<ReportExecutorStateResponse>, Status> {
        info!(
            "Got report_executor_state request from Executor with ID {}",
            request
                .get_ref()
                .executor_state
                .clone()
                .unwrap()
                .executor_id()
        );
        Ok(Response::new(ReportExecutorStateResponse {}))
    }

    async fn get_desired_executor_states(
        &self,
        request: Request<GetDesiredExecutorStatesRequest>,
    ) -> Result<Response<Self::get_desired_executor_statesStream>, Status> {
        info!(
            "Got get_desired_executor_states request from Executor with ID {}",
            request.get_ref().executor_id()
        );

        // Based on https://github.com/hyperium/tonic/blob/72b0fd59442d71804d4104e313ef6f140ab8f6d1/examples/src/streaming/server.rs#L46
        // creating infinite stream with fake message
        let repeat = std::iter::repeat(DesiredExecutorState {
            function_executors: vec![],
            task_allocations: vec![],
            clock: Some(3),
        });
        let mut stream = Box::pin(tokio_stream::iter(repeat));

        // spawn and channel are required if you want handle "disconnect" functionality
        // the `out_stream` will not be polled after client disconnect
        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                match tx.send(Result::<_, Status>::Ok(item)).await {
                    Ok(_) => {
                        // item (server response) was queued to be send to
                        // client
                    }
                    Err(_item) => {
                        // output_stream was build from rx and both are dropped
                        break;
                    }
                }
            }
            info!("get_desired_executor_states stream finished, client disconnected");
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::get_desired_executor_statesStream
        ))
    }
}
