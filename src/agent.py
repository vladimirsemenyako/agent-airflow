# agent.py

from dataclasses import dataclass
from pydantic import BaseModel, Field
from pydantic_ai import Agent, RunContext
import asyncio
import json
import logging
from devtools import pprint
import colorlog
from httpx import AsyncClient, HTTPStatusError

log_format = '%(log_color)s%(asctime)s [%(levelname)s] %(reset)s%(purple)s[%(name)s] %(reset)s%(blue)s%(message)s'
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(log_format))
logging.basicConfig(level=logging.INFO, handlers=[handler])

logger = logging.getLogger(__name__)

@dataclass  
class Deps:  
    airflow_api_base_uri: str  
    airflow_api_port: int  
    airflow_api_user: str  
    airflow_api_pass: str
    
class DAGStatus(BaseModel):  
    dag_id: str = Field(description='ID of the DAG')  
    dag_display_name: str = Field(description='Display name of the DAG')  
    is_paused: bool = Field(description='Whether the DAG is paused')  
    next_dag_run_data_interval_start: str = Field(description='Next DAG run data interval start')  
    next_dag_run_data_interval_end: str = Field(description='Next DAG run data interval end')  
    last_dag_run_id: str = Field(default='No DAG run', description='Last DAG run ID')  
    last_dag_run_state: str = Field(default='No DAG run', description='Last DAG run state')  
    total_dag_runs: int = Field(description='Total number of DAG runs')

airflow_agent = Agent(
    model='openai:gpt-4o',
    system_prompt=(
        'You are an Airflow monitoring assistant.\n'
        '**Workflow for Checking Status:**\n'
        '1. Use `list_dags` to find the correct DAG ID based on the user request.\n'
        '2. Use `get_dag_status` with the identified DAG ID to get its details.\n'
        '**Workflow for Triggering a DAG:**\n'
        '1. Use `list_dags` to find the correct DAG ID.\n'
        '2. Use the `trigger_dag` tool with the DAG ID to start a new run.\n'
        '3. After triggering, immediately call `get_dag_status` for the same DAG ID to get the latest status and use this for the final answer.'
    ),
    output_type=DAGStatus,
    deps_type=Deps,
    retries=2
)

@airflow_agent.tool
async def list_dags(ctx: RunContext[Deps]) -> str:
    """
    Get a list of all DAGs from the Airflow instance. Returns DAGs with their IDs and display names.
    """
    logger.info('Getting available DAGs...')
    uri = f'{ctx.deps.airflow_api_base_uri}:{ctx.deps.airflow_api_port}/api/v1/dags'
    auth = (ctx.deps.airflow_api_user, ctx.deps.airflow_api_pass)

    async with AsyncClient() as client:
        response = await client.get(uri, auth=auth)
        response.raise_for_status()

        dags_data = response.json()['dags']
        result = json.dumps([
            {'dag_id': dag['dag_id'], 'dag_display_name': dag['dag_display_name']} for dag in dags_data
        ])
        logger.debug(f'Available DAGs: {result}')
        return result
    
@airflow_agent.tool
async def get_dag_status(ctx: RunContext[Deps], dag_id: str) -> str:
    """
    Get detailed status information for a specific DAG by DAG ID.
    """
    logger.info(f'Getting status for DAG with ID: {dag_id}')
    base_url = f'{ctx.deps.airflow_api_base_uri}:{ctx.deps.airflow_api_port}/api/v1'
    auth = (ctx.deps.airflow_api_user, ctx.deps.airflow_api_pass)

    try:
        async with AsyncClient() as client:
            dag_response = await client.get(f'{base_url}/dags/{dag_id}', auth=auth)
            dag_response.raise_for_status()

            runs_response = await client.get(
                f'{base_url}/dags/{dag_id}/dagRuns',
                auth=auth,
                params={'order_by': '-execution_date', 'limit': 1}
            )
            runs_response.raise_for_status()

            result = {
                'dag_data': dag_response.json(),
                'runs_data': runs_response.json()
            }

            logger.debug(f'DAG status: {json.dumps(result)}')
            return json.dumps(result)

    except HTTPStatusError as e:
        if e.response.status_code == 404:
            return f'DAG with ID {dag_id} not found'
        raise

@airflow_agent.tool
async def trigger_dag(ctx: RunContext[Deps], dag_id: str) -> str:
    """
    Triggers a new run for a specific DAG by its ID.
    """
    logger.info(f'Triggering DAG with ID: {dag_id}')
    uri = f'{ctx.deps.airflow_api_base_uri}:{ctx.deps.airflow_api_port}/api/v1/dags/{dag_id}/dagRuns'
    auth = (ctx.deps.airflow_api_user, ctx.deps.airflow_api_pass)
    
    # API Airflow требует тело JSON, даже если оно пустое.
    payload = {"conf": {}}

    async with AsyncClient() as client:
        try:
            response = await client.post(uri, auth=auth, json=payload)
            response.raise_for_status()
            
            triggered_run_data = response.json()
            logger.info(f"Successfully triggered DAG {dag_id}. Run details: {json.dumps(triggered_run_data)}")
            # Возвращаем сообщение для LLM, чтобы он знал, что делать дальше
            return (
                f"Successfully triggered DAG '{dag_id}'. The new DAG run is "
                f"'{triggered_run_data.get('state', 'unknown')}'. "
                "Now, get the full, updated status using get_dag_status."
            )

        except HTTPStatusError as e:
            logger.error(f"Failed to trigger DAG {dag_id}: {e}")
            if e.response.status_code == 404:
                return f"DAG with ID '{dag_id}' not found."
            elif e.response.status_code == 409: # Конфликт - DAG уже запущен или на паузе
                return f"Failed to trigger DAG '{dag_id}'. A run is already active or the DAG is paused."
            return f"An error occurred while triggering DAG '{dag_id}': {e.response.text}"

async def main():
    deps = Deps(
        airflow_api_base_uri='http://localhost',
        airflow_api_port=8080,
        airflow_api_user='airflow',
        airflow_api_pass='airflow'
    )

    # Запрос на запуск DAG
    user_request = 'Can you please run the DAG for our daily payment report?'
    
    # Предыдущий запрос на получение статуса
    # user_request = 'What is the status of the DAG for our daily payment report?'

    print(f"User request: '{user_request}'")
    result = await airflow_agent.run(user_request, deps=deps)
    
    print("\n--- Agent Final Result ---")
    pprint(result.output)

if __name__ == "__main__":
    asyncio.run(main())