from src.preprocessing import preprocessing


def test_haversine_distance(capsys):
    distance_ = preprocessing.haversine_distance(0.4, 0.2, 0.5, 0.1)
    captured = capsys.readouterr()
    assert distance_ == 2.0
