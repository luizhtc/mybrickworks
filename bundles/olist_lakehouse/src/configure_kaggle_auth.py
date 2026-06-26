from databricks.sdk import WorkspaceClient

def main() -> None:
    token = dbutils.widgets.get("kaggle_api_token")
    w = WorkspaceClient()
    scopes = w.secrets.list_scopes()

    if len(list(filter(lambda x: x.name == "kaggle", scopes))) == 0:
        # Kaggle scope does not exist
        w.secrets.create_scope("kaggle")
        w.secrets.put_secret(scope="kaggle", key="KAGGLE_API_TOKEN", string_value=token)
    else:
        # Kaggle scope exists
        if len(list(filter(lambda x: x.key == "KAGGLE_API_TOKEN", w.secrets.list_secrets(scope="kaggle")))) == 0:
            # Kaggle scope exists but no KAGGLE_API_TOKEN
            w.secrets.put_secret(scope="kaggle", key="KAGGLE_API_TOKEN", string_value=token)


if __name__ == "__main__":
    main()
