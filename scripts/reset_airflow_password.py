# reset_airflow_password.py

from airflow.www.app import create_app
from werkzeug.security import generate_password_hash
from airflow import settings

app = create_app()
with app.app_context():
    session = settings.Session()

    # Dynamically get the User model from the app
    User = app.appbuilder.get_app.config['AUTH_USER_MODEL']
    
    # Lookup the user
    user = session.query(User).filter_by(username="sdarjunwadkar").first()

    if user:
        user.password = generate_password_hash("Test123", method="pbkdf2:sha256")
        session.commit()
        print("✅ Password updated to 'Test123'")
    else:
        print("❌ User not found")