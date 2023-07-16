open Lockfree

let int_list_eq l1 l2 =
  if List.length l1 <> List.length l2 then (
    print_endline "List lengths not equal";
    false)
  else
    let l1 = List.sort Int.compare l1 in
    let l2 = List.sort Int.compare l2 in
    l1 = l2

let print_int_list =
  let open Format in
  let pp_sep ppf () = pp_print_string ppf "; " in
  printf "[ %a ]\n%!" (pp_print_list ~pp_sep pp_print_int)

let tests_sequential =
  QCheck.
    [
      Test.make ~name:"add"
        QCheck.(list small_nat)
        (fun ladd ->
          print_int_list ladd;
          let bag = Lf_bag.create () in
          try
            List.iter (fun v -> Lf_bag.add bag v) ladd;
            true
          with exn ->
            Printexc.to_string exn |> print_endline;
            false);
      Test.make ~name:"try_remove_any on empty get None"
        QCheck.(list small_nat)
        (fun ladd ->
          assume (ladd <> []);
          let bag = Lf_bag.create () in
          let remove_op1 = Lf_bag.try_remove_any bag in
          (* With some elements added and then removed *)
          List.iter (fun v -> Lf_bag.add bag v) ladd;
          List.iter (fun _ -> ignore (Lf_bag.try_remove_any bag)) ladd;
          let remove_op2 = Lf_bag.try_remove_any bag in
          Option.is_none remove_op1 && Option.is_none remove_op2);
      Test.make ~name:"add_remove consistent" (list small_nat) (fun ladd ->
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
      QCheck.(list small_nat)
      ~small:(fun _ -> 10)
      (fun ladd ->
        print_endline "new list ladd";
        print_int_list ladd;
        let bag = Lf_bag.create ~num_domains:2 ~blk_sz:10 () in
        let flag = ref false in
        let producer =
          Domain.spawn (fun () ->
              List.iter (Lf_bag.add bag) ladd;
              flag := true)
        in
        let removed = ref [] in
        let count = ref (List.length ladd) in
        Printf.printf "count %d\n" !count;
        while !count > 0 do
          if !flag then decr count;
          match Lf_bag.try_remove_any bag with
          | None -> ()
          | Some v -> removed := v :: !removed
        done;
        let empty = Option.is_none (Lf_bag.try_remove_any bag) in
        Domain.join producer;
        print_endline "ladd";
        print_int_list ladd;
        print_endline "removed";
        print_int_list !removed;
        Format.printf "%a%!" (Lf_bag.pp_print_t Format.pp_print_int) bag;
        int_list_eq ladd !removed && empty);
  ]

let stress_test =
  [
    QCheck.Test.make ~name:"parallel add stress"
      QCheck.(list small_nat)
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
