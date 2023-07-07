open Lockfree

let int_list_eq l1 l2 =
  if List.length l1 <> List.length l2 then (
    print_endline "List lengths not equal";
    false)
  else
    let l1 = List.sort Int.compare l1 in
    let l2 = List.sort Int.compare l2 in
    l1 = l2

let tests_sequential =
  QCheck.
    [
      Test.make ~name:"add"
        QCheck.(list int)
        (fun ladd ->
          let bag = Lf_bag.create () in
          try
            List.iter (fun v -> Lf_bag.add bag v) ladd;
            true
          with exn ->
            Printexc.to_string exn |> print_endline;
            false);
      Test.make ~name:"try_remove_any on empty get None"
        QCheck.(list int)
        (fun ladd ->
          assume (ladd <> []);
          let bag = Lf_bag.create () in
          let remove_op1 = Lf_bag.try_remove_any bag in
          (* With some elements added and then removed *)
          List.iter (fun v -> Lf_bag.add bag v) ladd;
          List.iter (fun _ -> ignore (Lf_bag.try_remove_any bag)) ladd;
          let remove_op2 = Lf_bag.try_remove_any bag in
          Option.is_none remove_op1 && Option.is_none remove_op2);
      Test.make ~name:"add_remove consistent" (list int) (fun ladd ->
          let bag = Lf_bag.create () in
          List.iter (fun v -> Lf_bag.add bag v) ladd;
          let elems =
            List.filter_map (fun _ -> Lf_bag.try_remove_any bag) ladd
          in
          if Option.is_none (Lf_bag.try_remove_any bag) then
            int_list_eq elems ladd
          else false);
    ]

let tests_one_consumer_one_producer =
  [
    QCheck.Test.make ~name:"parallel add & remove"
      QCheck.(list int)
      (fun ladd ->
        let bag = Lf_bag.create ~num_domains:2 () in
        let flag = ref true in
        let producer =
          Domain.spawn (fun () ->
              List.iter (Lf_bag.add bag) ladd;
              flag := false)
        in
        let removed = ref [] in
        while !flag do
          match Lf_bag.try_remove_any bag with
          | None -> ()
          | Some v -> removed := v :: !removed
        done;
        let empty = Option.is_none (Lf_bag.try_remove_any bag) in
        Domain.join producer;
        int_list_eq ladd !removed && empty);
  ]

let stress_test =
  [
    QCheck.Test.make ~name:"parallel add stress"
      QCheck.(list int)
      (fun ladd ->
        let num_domains = Domain.recommended_domain_count () in
        let bag = Lf_bag.create ~num_domains () in
        let add_l _i =
          Domain.spawn (fun () ->
              try
                List.iter (fun v -> Lf_bag.add bag v) ladd;
                true
              with exn ->
                Printexc.to_string exn |> print_endline;
                false)
        in
        let ldomains = List.init num_domains add_l in
        List.for_all (fun d -> Domain.join d) ldomains);
  ]

let main () =
  let to_alcotest = List.map QCheck_alcotest.to_alcotest in
  Alcotest.run "LockFree Bag"
    [
      ("test_sequential", to_alcotest tests_sequential);
      ("one_cons_one_prod", to_alcotest tests_one_consumer_one_producer);
      ("two_domains", to_alcotest stress_test);
    ]
;;

main ()