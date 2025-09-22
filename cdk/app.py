from aws_cdk import App, Tags
from settings import StackSettings
from stack import HlsViStack

settings = StackSettings()

app = App()
stack = HlsViStack(
    app,
    settings.STACK_NAME,
    settings=settings,
    env={"account": settings.MCP_ACCOUNT_ID, "region": settings.MCP_ACCOUNT_REGION},
)

for k, v in dict(
    Project="hls",
    Stack=settings.STACK_NAME,
).items():
    Tags.of(app).add(k, v, apply_to_launched_instances=True)

app.synth()
