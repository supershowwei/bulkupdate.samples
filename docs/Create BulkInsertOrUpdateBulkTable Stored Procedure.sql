CREATE PROCEDURE [dbo].[BulkInsertOrUpdateBulkTable]
     @UpdatedTable [bulktabletype] READONLY
AS
     MERGE INTO [dbo].[BulkTable] [bt]
     USING @UpdatedTable [ut]
     ON [bt].[Id] = [ut].[Id]
         WHEN MATCHED
         THEN UPDATE SET [Text] = [ut].[Text]
         WHEN NOT MATCHED
         THEN INSERT VALUES ([ut].[Id], [ut].[Text]);
GO