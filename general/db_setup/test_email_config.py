#!/usr/bin/env python3
"""
Test email configuration with your Gmail setup

Usage:
    python test_email_config.py recipient@example.com
"""

import asyncio
import os
import sys
from pathlib import Path

# Add src to path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent / "src"))


async def test_email(recipient_email):
    """Test sending a password reset email"""
    
    print("📧 Testing Email Configuration")
    print("=" * 40)
    
    # Import email service after environment is loaded
    from src.app.auth.email_service import EmailService
    email_service = EmailService()
    
    # Check if email service is configured
    if not email_service.is_configured:
        print("❌ Email service not properly configured!")
        print("Current configuration:")
        print(f"  smtp_host: {email_service.smtp_server}")
        print(f"  smtp_port: {email_service.smtp_port}")
        print(f"  sender_email: {email_service.smtp_username}")
        print(f"  app_password: {'*' * len(email_service.smtp_password) if email_service.smtp_password else 'None'}")
        return False
    
    print("✅ Email service configuration found:")
    print(f"  Server: {email_service.smtp_server}:{email_service.smtp_port}")
    print(f"  From: {email_service.from_name} <{email_service.from_email}>")
    print(f"  To: {recipient_email}")
    print()
    
    try:
        print("Sending test password reset email...")
        
        # Generate a test token
        test_token = "test_token_123456789"
        base_url = "https://your-domain.com"  # Replace with your actual domain
        
        # Send the email
        success = await email_service.send_password_reset_email(
            to_email=recipient_email,
            reset_token=test_token,
            base_url=base_url,
            user_name="Test User"
        )
        
        if success:
            print("✅ Test email sent successfully!")
            print(f"Check {recipient_email} for the password reset email.")
            print(f"Reset link: {base_url}/reset-password?token={test_token}")
            return True
        else:
            print("❌ Failed to send email")
            return False
            
    except Exception as e:
        print(f"❌ Error sending email: {e}")
        return False


def main():
    """Main function"""
    if len(sys.argv) != 2:
        print("Usage: python test_email_config.py recipient@example.com")
        print("Example: python test_email_config.py your.email@gmail.com")
        sys.exit(1)
    
    recipient = sys.argv[1]
    
    # Load environment variables from .env if it exists
    env_file = Path(".env")
    if env_file.exists():
        print("Loading .env file...")
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#') and line:
                    key, value = line.split('=', 1)
                    # Remove quotes if present
                    value = value.strip('"').strip("'")
                    os.environ[key] = value
                    print(f"  Loaded: {key}={value[:20]}..." if len(value) > 20 else f"  Loaded: {key}={value}")
    
    success = asyncio.run(test_email(recipient))
    
    if success:
        print("\n🎉 Email configuration is working!")
        print("Password reset emails will now be sent to users.")
    else:
        print("\n❌ Email test failed.")
        print("Please check your .env configuration.")


if __name__ == "__main__":
    main()