using FileReducer;

FileHashDb db = new();
await db.HashFolder("C:/Users/blend/Downloads");
//await db.HashFolder(@"C:\Users\blend\Downloads\asus");

db.ListDuplicates();