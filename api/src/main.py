import datetime
import os
from datetime import datetime as dt
from datetime import timedelta, timezone

import httpx
import psycopg
import uvicorn
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

app = FastAPI(title="Destiny Tracker API")

current_dir = os.path.dirname(os.path.abspath(__file__))
templates_dir = os.path.join(os.path.dirname(current_dir), "templates")
templates = Jinja2Templates(directory=templates_dir)

BUNGIE_CLIENT_ID = os.environ.get("BUNGIE_CLIENT_ID")
BUNGIE_CLIENT_SECRET = os.environ.get("BUNGIE_CLIENT_SECRET")
BUNGIE_API_KEY = os.environ.get("BUNGIE_API_KEY")

DB_HOST = os.environ.get("POSTGRES_HOST", "postgres")
DB_USER = os.environ.get("POSTGRES_USER")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
DB_NAME = os.environ.get("POSTGRES_DB")


def get_db_connection():
    return psycopg.connect(
        f"host={DB_HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"
    )


@app.get("/player/{discord_id}", response_class=HTMLResponse)
async def get_player_card(request: Request, discord_id: str):
    """
    Renders an HTML dashboard of a user's Dungeon and Raid achievements.
    """

    with get_db_connection() as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            # 1. Look up the Bungie Account ID associated with this Discord User
            cur.execute(
                "SELECT bungie_membership_id FROM config.tracked_users WHERE discord_id = %s",
                (discord_id,),
            )
            user_record = cur.fetchone()

            if not user_record:
                raise HTTPException(
                    status_code=404,
                    detail="User not registered in the tracker.",
                )

            account_id = user_record["bungie_membership_id"]

            # 2. Query Dungeon Aggregates
            # A solo flawless is just a solo run where deaths = 0
            cur.execute(
                """
                SELECT
                    activity_hash,
                    TRUE as has_solo,
                    bool_or(deaths = 0) as has_flawless
                FROM gold.gld_solo_dungeons
                WHERE account_id = %s
                GROUP BY activity_hash
                ORDER BY activity_hash
            """,
                (account_id,),
            )
            dungeon_data = cur.fetchall()

            # 3. Query Raid Aggregates
            # Evaluates the lowest player count achieved for each specific raid
            cur.execute(
                """
                SELECT
                    activity_hash,
                    bool_or(player_count <= 4) as man_4,
                    bool_or(player_count <= 3) as man_3,
                    bool_or(player_count <= 2) as man_2,
                    bool_or(player_count = 1) as man_1
                FROM gold.gld_lowman_raids
                WHERE account_id = %s
                GROUP BY activity_hash
                ORDER BY activity_hash
            """,
                (account_id,),
            )
            raid_data = cur.fetchall()

    # 4. Render the data into the HTML template
    return templates.TemplateResponse(
        "player_card.html",
        {
            "request": request,
            "discord_id": discord_id,
            "dungeons": dungeon_data,
            "raids": raid_data,
        },
    )


@app.get("/auth/login")
async def login(discord_id: str):
    if not discord_id:
        raise HTTPException(status_code=400, detail="discord_id is required")

    auth_url = (
        f"https://www.bungie.net/en/OAuth/Authorize"
        f"?client_id={BUNGIE_CLIENT_ID}"
        f"&response_type=code"
        f"&state={discord_id}"
    )
    return RedirectResponse(url=auth_url)


@app.get("/auth/callback")
async def auth_callback(code: str, state: str):
    discord_id = state

    token_url = "https://www.bungie.net/Platform/App/OAuth/Token/"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": BUNGIE_CLIENT_ID,
        "client_secret": BUNGIE_CLIENT_SECRET,
    }

    async with httpx.AsyncClient() as client:
        token_resp = await client.post(token_url, data=data, headers=headers)

        if token_resp.status_code != 200:
            raise HTTPException(
                status_code=400, detail="Failed to retrieve token from Bungie"
            )

        token_data = token_resp.json()

    access_token = token_data["access_token"]
    refresh_token = token_data["refresh_token"]
    expires_in = token_data["expires_in"]
    token_expires_at = dt.now(timezone.utc) + timedelta(seconds=expires_in)

    profile_url = (
        "https://www.bungie.net/Platform/User/GetMembershipsForCurrentUser/"
    )

    assert BUNGIE_API_KEY and access_token

    auth_headers = {
        "X-API-Key": BUNGIE_API_KEY,
        "Authorization": f"Bearer {access_token}",
    }

    async with httpx.AsyncClient() as client:
        profile_resp = await client.get(profile_url, headers=auth_headers)
        profile_data = profile_resp.json()

    destiny_memberships = profile_data.get("Response", {}).get(
        "destinyMemberships", []
    )
    if not destiny_memberships:
        raise HTTPException(
            status_code=400,
            detail="No Destiny 2 account found linked to this profile.",
        )

    primary_membership_id = profile_data.get("Response", {}).get(
        "primaryMembershipId"
    )
    selected_membership = destiny_memberships[0]

    if primary_membership_id:
        for membership in destiny_memberships:
            if membership.get("membershipId") == primary_membership_id:
                selected_membership = membership
                break

    membership_id = selected_membership["membershipId"]
    membership_type = selected_membership["membershipType"]

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO config.tracked_users
                (discord_id, bungie_membership_id, bungie_membership_type, access_token, refresh_token, token_expires_at, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, TRUE)
                ON CONFLICT (discord_id) DO UPDATE SET
                    bungie_membership_id = EXCLUDED.bungie_membership_id,
                    bungie_membership_type = EXCLUDED.bungie_membership_type,
                    access_token = EXCLUDED.access_token,
                    refresh_token = EXCLUDED.refresh_token,
                    token_expires_at = EXCLUDED.token_expires_at,
                    is_active = TRUE
            """,
                (
                    discord_id,
                    membership_id,
                    membership_type,
                    access_token,
                    refresh_token,
                    token_expires_at,
                ),
            )
        conn.commit()

    return {
        "message": "Success! Your Destiny 2 account has been linked. You can close this window and return to Discord."
    }


if __name__ == "__main__":
    ssl_dir_path = "/tmp/ssl"
    cert_path = os.path.join(ssl_dir_path, "localhost.crt")
    key_path = os.path.join(ssl_dir_path, "localhost.key")

    if not os.path.exists(ssl_dir_path):
        os.makedirs(ssl_dir_path)

    if not os.path.exists(cert_path) or not os.path.exists(key_path):
        key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )

        subject = issuer = x509.Name(
            [
                x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
                x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "Virginia"),
                x509.NameAttribute(NameOID.LOCALITY_NAME, "Charlottesville"),
                x509.NameAttribute(
                    NameOID.ORGANIZATION_NAME, "SelfSigned Inc."
                ),
                x509.NameAttribute(NameOID.COMMON_NAME, "localhost"),
            ]
        )

        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
            .not_valid_after(
                datetime.datetime.now(datetime.timezone.utc)
                + timedelta(days=365)
            )
            .add_extension(
                x509.SubjectAlternativeName([x509.DNSName("localhost")]),
                critical=False,
            )
            .sign(key, hashes.SHA256())
        )

        with open(key_path, "wb") as f:
            f.write(
                key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.TraditionalOpenSSL,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            )

        with open(cert_path, "wb") as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))

    # Run Uvicorn securely
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        ssl_keyfile=key_path,
        ssl_certfile=cert_path,
        reload=True,
    )
