"""Script to help set up Schwab OAuth tokens for ThetaGang."""
import asyncio
import sys
from pathlib import Path

try:
    from schwab import AsyncSchwabClient
except ImportError:
    print("Error: schwab-trader library not installed")
    print("Install with: pip install schwab-trader")
    sys.exit(1)


async def setup_oauth():
    """Interactive OAuth setup for Schwab."""
    print("=" * 70)
    print("ThetaGang - Schwab OAuth Setup")
    print("=" * 70)
    print()
    print("This script will help you authenticate with the Schwab API.")
    print("You will need:")
    print("  1. App Key from https://developer.schwab.com")
    print("  2. App Secret from https://developer.schwab.com")
    print("  3. Redirect URI configured in your Schwab app")
    print()
    print("=" * 70)
    print()

    # Get credentials from user
    app_key = input("Enter your Schwab App Key: ").strip()
    if not app_key:
        print("Error: App Key is required")
        sys.exit(1)

    app_secret = input("Enter your Schwab App Secret: ").strip()
    if not app_secret:
        print("Error: App Secret is required")
        sys.exit(1)

    redirect_uri = (
        input("Enter your redirect URI (default: https://127.0.0.1:8182): ").strip()
        or "https://127.0.0.1:8182"
    )

    # Set up token storage path
    token_path = Path.home() / ".thetagang" / "schwab_tokens.json"
    token_path.parent.mkdir(parents=True, exist_ok=True)

    print()
    print(f"Tokens will be stored at: {token_path}")
    print()
    print("=" * 70)
    print("Starting OAuth flow...")
    print("=" * 70)
    print()
    print("A browser window will open for you to log in to Schwab.")
    print("After logging in, you'll be redirected to a page that may not load.")
    print("That's OK! Just copy the entire URL from your browser's address bar.")
    print()

    try:
        async with AsyncSchwabClient(
            api_key=app_key,
            api_secret=app_secret,
            redirect_uri=redirect_uri,
            token_path=str(token_path),
        ) as client:
            # Get account numbers to verify connection
            print("Fetching account information...")
            accounts = await client.get_account_numbers()

            print()
            print("=" * 70)
            print("✓ Successfully authenticated!")
            print("=" * 70)
            print()
            print(f"Found {len(accounts)} account(s):")
            print()

            for i, account in enumerate(accounts, 1):
                print(f"Account {i}:")
                print(f"  Account Number: {account.accountNumber}")
                print(f"  Hash Value: {account.hashValue}")
                print()

            print("=" * 70)
            print("Setup Complete!")
            print("=" * 70)
            print()
            print(f"✓ Tokens saved to: {token_path}")
            print()
            print("Next steps:")
            print("1. Copy one of the 'Hash Value' entries above")
            print("2. Add it to your thetagang.toml config file:")
            print()
            print("   [account]")
            print("   broker = \"schwab\"")
            print()
            print("   [account.schwab]")
            print(f'   app_key = "{app_key}"')
            print(f'   app_secret = "{app_secret}"')
            print(f'   redirect_uri = "{redirect_uri}"')
            print('   account_number = "PASTE_HASH_VALUE_HERE"')
            print()
            print("3. Run ThetaGang: thetagang --config thetagang.toml")
            print()

    except KeyboardInterrupt:
        print("\n\nSetup cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nError during setup: {e}")
        print("\nPlease check:")
        print("  - Your app credentials are correct")
        print("  - The redirect URI matches your Schwab app configuration")
        print("  - You have an active internet connection")
        sys.exit(1)


def main():
    """Main entry point."""
    try:
        asyncio.run(setup_oauth())
    except KeyboardInterrupt:
        print("\n\nSetup cancelled by user")
        sys.exit(1)


if __name__ == "__main__":
    main()
