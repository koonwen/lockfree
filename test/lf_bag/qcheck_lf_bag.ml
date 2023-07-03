open Lockfree__.Lf_bag

let test1 =
  QCheck.Test.make ~name:"Sequential add and remove"
    QCheck.(list small_nat)
    (fun l ->
      let bag = S.create ~num_domains:1 () in
      List.iter (fun v -> S.add bag v) l;
      List.fold_left
        (fun acc _ -> Option.get (S.try_remove_any bag) :: acc)
        [] l
      = l)

let test2 =
  QCheck.Test.make ~name:"Concurrent add"
    ~small:(fun _ -> 100)
    QCheck.(list small_nat)
    (fun l ->
      let bag = S.create ~num_domains:2 () in
      let add_l = List.iter (S.add bag) in

      let d1 = Domain.spawn (fun () -> add_l l) in
      let d2 = Domain.spawn (fun () -> add_l l) in
      Domain.join d1;
      Domain.join d2;

      true)
;;

(* we can check right now the property... *)
QCheck_runner.run_tests ~verbose:true [ test1; test2 ]

(* let () =
     let suite = List.map QCheck_alcotest.to_alcotest [ simple_test ] in
     Alcotest.run "my test" [ ("suite", suite) ] *)
