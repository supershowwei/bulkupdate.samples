CREATE PROCEDURE [dbo].[BulkUpdateBulkTable]
     @UpdatedTable [BulkTableType] READONLY
AS
     UPDATE [dbo].[BulkTable]
       SET
           [Text] = [ut].[Text]
     FROM [dbo].[BulkTable] [bt]
          JOIN @UpdatedTable [ut] ON [bt].[Id] = [ut].[Id]
GO