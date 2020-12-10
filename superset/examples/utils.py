from pathlib import Path
from typing import Any, Dict

from pkg_resources import resource_isdir, resource_listdir, resource_stream

from superset.commands.importers.v1.examples import ImportExamplesCommand


def load_from_configs() -> None:
    contents = load_contents()
    command = ImportExamplesCommand(contents)
    command.run()


def load_contents() -> Dict[str, Any]:
    """Traverse configs directory and load contents"""
    root = Path("examples/configs")
    resource_names = resource_listdir("superset", str(root))
    queue = [root / resource_name for resource_name in resource_names]

    contents: Dict[Path, str] = {}
    while queue:
        path_name = queue.pop()

        if resource_isdir("superset", str(path_name)):
            queue.extend(
                path_name / child_name
                for child_name in resource_listdir("superset", str(path_name))
            )
        else:
            contents[path_name] = (
                resource_stream("superset", str(path_name)).read().decode("utf-8")
            )

    return {str(path.relative_to(root)): content for path, content in contents.items()}
