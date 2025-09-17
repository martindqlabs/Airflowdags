# agent/loader_agent.py

from crewai import Agent, Task, Crew
from langchain.chat_models import ChatOpenAI
import json
import os
import re
from dotenv import load_dotenv

load_dotenv()
print("DEEPSEEK_API_KEY:", os.getenv("DEEPSEEK_API_KEY"))


def generate_sample_data(preview_data):
    # Load DeepSeek credentials
    DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
    DEEPSEEK_API_BASE = "https://api.deepseek.com/v1"

    if not DEEPSEEK_API_KEY:
        raise EnvironmentError("DEEPSEEK_API_KEY not found in environment variables.")

    sample_text = str(preview_data)

    # Initialize DeepSeek via LangChain (bypassing LiteLLM)
    deepseek_llm = ChatOpenAI(
        model="deepseek-chat",
        openai_api_key=DEEPSEEK_API_KEY,
        openai_api_base=DEEPSEEK_API_BASE,
        temperature=0.7
    )

    # Create CrewAI agent
    data_generator = Agent(
        role='Data Generation Specialist',
        goal='Generate realistic sample data based on existing records',
        backstory='Expert in data generation and analysis with deep understanding of data patterns',
        verbose=True,
        allow_delegation=False,
        llm={
            "provider": "deepseek",        # specify the provider explicitly
            "api_key": os.getenv("DEEPSEEK_API_KEY"),
            "model": "deepseek-chat",
            "api_base": "https://api.deepseek.com/v1"
        }  # Not using llm_config to avoid LiteLLM
    )

    # Define the task
    generate_task = Task(
        description=(
            "Given the following records from a database table:\n"
            f"{sample_text}\n"
            "Generate 10 new sample records with similar structure and plausible values "
            "in JSON format as a list of dictionaries."
        ),
        agent=data_generator,
        expected_output="A JSON string containing 10 sample records"
    )

    # Run the crew
    crew = Crew(
        agents=[data_generator],
        tasks=[generate_task],
        verbose=True
    )

    result = crew.kickoff()

    # Handle output
    try:
        return json.loads(result)
    except json.JSONDecodeError:
        json_match = re.search(r'\[.*\]', result, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group())
            except json.JSONDecodeError:
                raise ValueError(f"JSON extraction failed. Raw output:\n{result}")
        else:
            raise ValueError(f"Could not parse generated data as JSON. Raw output:\n{result}")
