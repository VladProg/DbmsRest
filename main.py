from fastapi import FastAPI, HTTPException, status
from models import *

app = FastAPI(title='DBMS')

databases: dict[str, Database] = {}


class HTTPError(BaseModel):
    detail: str


def response(description, example=None, **examples):
    assert (not example) != (not examples)
    result = {
        "example": {
            "detail": example
        }
    } if example else {
        "examples": {
            key: {
                "summary": key.replace('_', ' '),
                "value": {"detail": value},
            }
            for key, value in examples.items()
        }
    }
    return {
        "model": HTTPError,
        "description": description,
        "content": {
            "application/json": result
        }
    }


@app.get('/databases')
def get_databases() -> list[str]:
    return list(databases)


@app.get('/databases/{database_name}', response_model_exclude_none=True, responses={
    status.HTTP_404_NOT_FOUND: response("Database was not found", "Cannot find database 'database_name'"),
})
def get_database(database_name: str) -> Database:
    try:
        return databases[database_name]
    except KeyError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Cannot find database '{database_name}'")


class NewDatabase(BaseModel):
    name: str


@app.post('/databases', status_code=status.HTTP_201_CREATED, responses={
    status.HTTP_409_CONFLICT: response("Database already exists", "Database 'database_name' already exists"),
})
def create_database(new_database: NewDatabase) -> Database:
    if new_database.name in databases:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                            detail=f"Database '{new_database.name}' already exists")
    database = Database(name=new_database.name)
    databases[new_database.name] = database
    return database


@app.delete('/databases/{database_name}', status_code=status.HTTP_204_NO_CONTENT, responses={
    status.HTTP_404_NOT_FOUND: response("Database was not found", "Cannot find database 'database_name'"),
})
def delete_database(database_name: str):
    get_database(database_name)
    del databases[database_name]


@app.get('/databases/{database_name}/tables/{table_id}', response_model_exclude_none=True, responses={
    status.HTTP_404_NOT_FOUND: response("Database or table was not found",
                                        database_not_found="Cannot find database 'database_name'",
                                        table_not_found="Database 'database_name' doesn't contain table #table_id"),
})
def get_table(database_name: str, table_id: NonNegativeInt) -> Table:
    database = get_database(database_name)
    try:
        return database.tables[table_id]
    except KeyError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Database '{database_name}' doesn't contain table #{table_id}")


class NewTable(BaseModel):
    name: str
    columns: list[Column]


@app.post('/databases/{database_name}/tables/',
          response_model_exclude_none=True,
          status_code=status.HTTP_201_CREATED,
          responses={
              status.HTTP_404_NOT_FOUND: response("Database was not found", "Cannot find database 'database_name'"),
          })
def create_table(database_name: str, new_table: NewTable) -> Table:
    database = get_database(database_name)
    return database.add_table(new_table.name, new_table.columns)


@app.delete('/databases/{database_name}/tables/{table_id}', status_code=status.HTTP_204_NO_CONTENT, responses={
    status.HTTP_404_NOT_FOUND: response("Database or table was not found",
                                        database_not_found="Cannot find database 'database_name'",
                                        table_not_found="Database 'database_name' doesn't contain table #table_id"),
})
def delete_table(database_name: str, table_id: NonNegativeInt):
    get_table(database_name, table_id)
    get_database(database_name).remove_table(table_id)


@app.get('/databases/{database_name}/tables/{table_id}/rows', responses={
    status.HTTP_404_NOT_FOUND: response("Database or table was not found",
                                        database_not_found="Cannot find database 'database_name'",
                                        table_not_found="Database 'database_name' doesn't contain table #table_id"),
})
def get_table_rows(database_name: str, table_id: NonNegativeInt) -> dict[NonNegativeInt, Row]:
    return get_table(database_name, table_id)._rows


@app.get('/databases/{database_name}/tables/{table_id}/rows/{row_id}', responses={
    status.HTTP_404_NOT_FOUND: response(
        "Database, table or row was not found",
        database_not_found="Cannot find database 'database_name'",
        table_not_found="Database 'database_name' doesn't contain table #table_id",
        row_not_found="Table #table_id in database 'database_name' doesn't contain row #row_id"),
})
def get_row(database_name: str, table_id: NonNegativeInt, row_id: NonNegativeInt) -> Row:
    try:
        return get_table(database_name, table_id)._rows[row_id]
    except KeyError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Table #{table_id} in database '{database_name}' doesn't contain row #{row_id}")


class NewRow(BaseModel):
    cells: list[Value]


@app.post('/databases/{database_name}/tables/{table_id}/rows', status_code=status.HTTP_201_CREATED, responses={
    status.HTTP_404_NOT_FOUND: response("Database or table was not found",
                                        database_not_found="Cannot find database 'database_name'",
                                        table_not_found="Database 'database_name' doesn't contain table #table_id"),
    status.HTTP_400_BAD_REQUEST: response("Invalid row",
                                          incorrect_row_length="Row length must be the same as number of columns",
                                          invalid_value="Type1 expected but Type2 value 'value' found"),
})
def create_row(database_name: str, table_id: NonNegativeInt, new_row: NewRow) -> Row:
    try:
        return get_table(database_name, table_id).add_row(new_row.cells)
    except ValueError as err:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(err))


@app.delete(
    '/databases/{database_name}/tables/{table_id}/rows/{row_id}',
    status_code=status.HTTP_204_NO_CONTENT, responses={
        status.HTTP_404_NOT_FOUND: response(
            "Database, table or row was not found",
            database_not_found="Cannot find database 'database_name'",
            table_not_found="Database 'database_name' doesn't contain table #table_id",
            row_not_found="Table #table_id in database 'database_name' doesn't contain row #row_id"),
    })
def delete_row(database_name: str, table_id: NonNegativeInt, row_id: NonNegativeInt):
    get_row(database_name, table_id, row_id)
    get_table(database_name, table_id).remove_row(row_id)


@app.patch('/databases/{database_name}/tables/{table_id}/rows/{row_id}/cells/{column_id}', responses={
    status.HTTP_404_NOT_FOUND: response(
        "Database, table, row or column was not found",
        database_not_found="Cannot find database 'database_name'",
        table_not_found="Database 'database_name' doesn't contain table #table_id",
        row_not_found="Table #table_id in database 'database_name' doesn't contain row #row_id",
        column_not_found="Table #table_id in database 'database_name' doesn't contain column #column_id"),
    status.HTTP_400_BAD_REQUEST: response("Invalid value", "Type1 expected but Type2 value 'value' found"),
})
def update_cell_value(
        database_name: str,
        table_id: NonNegativeInt,
        row_id: NonNegativeInt,
        column_id: NonNegativeInt,
        value: Value) -> Row:
    table = get_table(database_name, table_id, )
    row = get_row(database_name, table_id, row_id)
    if column_id >= len(table.columns):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table #{table_id} in database '{database_name}' doesn't contain column #{column_id}")
    try:
        table.columns[column_id].check_value(value)
    except ValueError as err:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(err))
    row.cells[column_id] = value
    return row


@app.get('/databases/{database_name}/table_difference/{left_table_id}/{right_table_id}',
         response_model_exclude_none=True,
         responses={
             status.HTTP_404_NOT_FOUND: response(
                 "Database or table was not found",
                 database_not_found="Cannot find database 'database_name'",
                 table_not_found="Database 'database_name' doesn't contain table #table_id"),
             status.HTTP_400_BAD_REQUEST: response(
                 "Tables are incompatible",
                 different_column_counts="Table difference: tables have different column counts",
                 different_column_types="Table difference: tables have different column types"),
         })
def get_table_difference(database_name: str, left_table_id: NonNegativeInt,
                         right_table_id: NonNegativeInt) -> TableDifference:
    left_table = get_table(database_name, left_table_id)
    right_table = get_table(database_name, right_table_id)
    return left_table - right_table


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
