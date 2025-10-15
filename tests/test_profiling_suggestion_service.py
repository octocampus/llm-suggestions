import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.db.schema import Base
from app.services.profiling_suggestion_service import ProfilingSuggestionService
from app.model.profiling_suggestion import ProfilingSuggestionCreate

# In-memory database for tests
TEST_DATABASE_URL = "sqlite:///:memory:"


@pytest.fixture
def test_db():
    """Create test database"""
    engine = create_engine(TEST_DATABASE_URL, connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    TestSessionLocal = sessionmaker(bind=engine)
    db = TestSessionLocal()
    yield db
    db.close()


def test_create_suggestion(test_db):
    """Test creating a suggestion"""
    service = ProfilingSuggestionService(test_db)
    
    suggestion = ProfilingSuggestionCreate(
        dataset_name="users",
        column_name="email",
        suggestion_type="validation",
        suggestion_text="Add email validation",
        severity="warning"
    )
    
    result = service.create_suggestion(suggestion)
    
    assert result.id is not None
    assert result.dataset_name == "users"
    assert result.severity == "warning"


def test_get_suggestions_with_filter(test_db):
    """Test filtering suggestions"""
    service = ProfilingSuggestionService(test_db)
    
    # Create test data
    service.create_suggestion(ProfilingSuggestionCreate(
        dataset_name="users",
        column_name="age",
        suggestion_type="quality",
        suggestion_text="Check for outliers",
        severity="critical"
    ))
    
    # Query
    results = service.get_suggestions(dataset_name="users", severity="critical")
    
    assert len(results) == 1
    assert results[0].severity == "critical"