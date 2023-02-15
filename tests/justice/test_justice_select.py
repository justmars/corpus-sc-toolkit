import pytest

from corpus_sc_toolkit import CandidateJustice, JusticeDetail


@pytest.fixture
def candidate_list(session):
    return CandidateJustice(
        db=session,
        date_str="Dec. 1, 1995",
    )


@pytest.fixture
def candidate(session):
    return CandidateJustice(
        db=session,
        text="Panganiban, Acting Cj",
        date_str="Dec. 1, 1995",
    )


@pytest.fixture
def candidate_as_cj(session):
    return CandidateJustice(
        db=session, text="Panganiban", date_str="2006-03-30"
    )


def test_justice_candidate(candidate_list):
    assert candidate_list.rows == [
        {
            "id": 137,
            "surname": "panganiban",
            "alias": None,
            "start_term": "1995-10-05",
            "inactive_date": "2006-12-06",
            "chief_date": "2005-12-20",
        },
        {
            "id": 136,
            "surname": "hermosisima",
            "alias": "hermosisima jr.",
            "start_term": "1995-01-10",
            "inactive_date": "1997-10-18",
            "chief_date": None,
        },
        {
            "id": 135,
            "surname": "francisco",
            "alias": None,
            "start_term": "1995-01-05",
            "inactive_date": "1998-02-13",
            "chief_date": None,
        },
        {
            "id": 134,
            "surname": "mendoza",
            "alias": None,
            "start_term": "1994-06-07",
            "inactive_date": "2003-04-05",
            "chief_date": None,
        },
        {
            "id": 133,
            "surname": "kapunan",
            "alias": None,
            "start_term": "1994-01-05",
            "inactive_date": "2002-08-12",
            "chief_date": None,
        },
        {
            "id": 132,
            "surname": "vitug",
            "alias": None,
            "start_term": "1993-06-28",
            "inactive_date": "2004-07-15",
            "chief_date": None,
        },
        {
            "id": 131,
            "surname": "puno",
            "alias": None,
            "start_term": "1993-06-28",
            "inactive_date": "2010-05-17",
            "chief_date": "2007-12-08",
        },
        {
            "id": 128,
            "surname": "melo",
            "alias": None,
            "start_term": "1992-08-10",
            "inactive_date": "2002-05-30",
            "chief_date": None,
        },
        {
            "id": 127,
            "surname": "bellosillo",
            "alias": None,
            "start_term": "1992-03-03",
            "inactive_date": "2003-11-13",
            "chief_date": None,
        },
        {
            "id": 125,
            "surname": "romero",
            "alias": None,
            "start_term": "1991-10-21",
            "inactive_date": "1999-08-01",
            "chief_date": None,
        },
        {
            "id": 124,
            "surname": "davide",
            "alias": "davide jr.",
            "start_term": "1991-01-24",
            "inactive_date": "2005-12-20",
            "chief_date": "1998-11-30",
        },
        {
            "id": 123,
            "surname": "regalado",
            "alias": None,
            "start_term": "1988-07-29",
            "inactive_date": "1998-10-13",
            "chief_date": None,
        },
        {
            "id": 116,
            "surname": "padilla",
            "alias": None,
            "start_term": "1987-01-12",
            "inactive_date": "1997-08-22",
            "chief_date": None,
        },
        {
            "id": 115,
            "surname": "feliciano",
            "alias": None,
            "start_term": "1986-08-08",
            "inactive_date": "1995-12-13",
            "chief_date": None,
        },
        {
            "id": 112,
            "surname": "narvasa",
            "alias": None,
            "start_term": "1986-04-10",
            "inactive_date": "1998-11-30",
            "chief_date": "1991-12-08",
        },
    ]


def test_justice_choice(candidate):
    assert candidate.choice == {
        "id": 137,
        "surname": "Panganiban",
        "start_term": "1995-10-05",
        "inactive_date": "2006-12-06",
        "chief_date": "2005-12-20",
        "designation": "J.",
    }


def test_justice_ponencia(candidate):
    assert candidate.ponencia == {
        "justice_id": 137,
        "raw_ponente": "Panganiban",
        "per_curiam": False,
    }


def test_justice_detail_generic_designation(candidate):
    assert candidate.detail == JusticeDetail(
        justice_id=137,
        raw_ponente="Panganiban",
        designation="J.",
        per_curiam=False,
    )


def test_justice_detail_cj_designation(candidate_as_cj):
    assert candidate_as_cj.detail == JusticeDetail(
        justice_id=137,
        raw_ponente="Panganiban",
        designation="C.J.",
        per_curiam=False,
    )
