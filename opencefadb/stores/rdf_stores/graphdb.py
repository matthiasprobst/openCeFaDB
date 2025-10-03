from .graphdb import GraphDB
from .graphdb.repository import GraphDBRepository


def connect_to_graphdb(url: str, repo_name: str, auth=(None, None)) -> GraphDBRepository:
    gdb = GraphDB(url=url, auth=auth)
    return gdb[repo_name]
