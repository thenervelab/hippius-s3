from pydantic import BaseModel


class HippiusAccount(BaseModel):
    id: str
    main_account: str
    upload: bool
    delete: bool
    has_credits: bool
