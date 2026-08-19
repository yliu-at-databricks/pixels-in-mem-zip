from pyspark.sql import SparkSession

from conftest import (
    CHECKPOINT_BASE_PATH,
    TABLE,
    UNZIP_BASE_PATH,
    VOLUME_UC,
    ZIP_FILE_PATH,
)
from dbx.pixels import Catalog


def test_catalog_unzip(spark: SparkSession):
    """Test in-memory zip extraction (no disk extraction)."""
    catalog = Catalog(spark, table=TABLE, volume=VOLUME_UC)
    catalog_df = catalog.catalog(
        path=ZIP_FILE_PATH, extractZip=True
    )

    assert catalog_df is not None
    # content column should be present when extractZip=True (in-memory mode)
    assert "content" in catalog_df.columns

    # Drop content before saving to avoid persisting large binary blobs
    catalog.save(df=catalog_df.drop("content"))

    assert catalog.load().count() == 30


def test_catalog_unzip_stream(spark: SparkSession):
    """Test in-memory zip extraction in streaming mode."""
    catalog = Catalog(spark, table=TABLE, volume=VOLUME_UC)
    catalog_df = catalog.catalog(
        path=ZIP_FILE_PATH,
        extractZip=True,
        streaming=True,
        streamCheckpointBasePath=CHECKPOINT_BASE_PATH,
    )

    assert catalog_df is not None

    # Drop content before saving to avoid persisting large binary blobs
    catalog.save(df=catalog_df.drop("content"))

    assert catalog.load().count() == 30
