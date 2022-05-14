use fxkv::db::Db;

fn main() {
    let db = Db::new().unwrap();
    let trees = db
        .start_transaction(["tree1", "tree2"].into_iter())
        .unwrap();
    let t1 = trees.get(0);
    t1.set("key1", "value".as_bytes().to_vec()).unwrap();
    t1.remove("key1").unwrap();
    trees.commit().unwrap();
}
