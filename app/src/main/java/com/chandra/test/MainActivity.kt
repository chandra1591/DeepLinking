package com.chandra.test

import android.os.Bundle
import androidx.activity.enableEdgeToEdge
import androidx.appcompat.app.AppCompatActivity
import androidx.core.view.ViewCompat
import androidx.core.view.WindowInsetsCompat
import android.content.Intent
import android.net.Uri
import android.util.Log
import io.reactivex.rxjava3.core.Completable
import io.reactivex.rxjava3.core.CompletableObserver
import io.reactivex.rxjava3.core.Maybe
import io.reactivex.rxjava3.core.MaybeObserver
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Observer
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.core.SingleObserver
import io.reactivex.rxjava3.disposables.CompositeDisposable
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.functions.BiFunction
import io.reactivex.rxjava3.schedulers.Timed
import java.util.Locale
import java.util.concurrent.TimeUnit

class MainActivity : AppCompatActivity() {
    private val compositeDisposable = CompositeDisposable()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        setContentView(R.layout.activity_main)
        ViewCompat.setOnApplyWindowInsetsListener(findViewById(R.id.main)) { v, insets ->
            val systemBars = insets.getInsets(WindowInsetsCompat.Type.systemBars())
            v.setPadding(systemBars.left, systemBars.top, systemBars.right, systemBars.bottom)
            insets
        }
        // ATTENTION: This was auto-generated to handle app links.
        val appLinkIntent: Intent = intent
        val appLinkAction: String? = appLinkIntent.action
        val appLinkData: Uri? = appLinkIntent.data

        Log.e(TAG, "Action = $appLinkAction")
        Log.e(TAG, "Data = $appLinkData")

//        showDataFlowRXJava()
//        singleObservableXJava()
//        completeObservableRxJava()
//        mayBeObservableRxJava()

//        RX Operators
//        rxJavaOperatorRange()
//        rxJavaOperatorRepeat()
        OperatorExample()
        filterningObservable()
        combiningObservable()

//        error handling operator
        errorHandlingOperator()
//        Utility Operator
        utilityOperators()

//        Cold Hot and Connectable observable
        coldHotConnectedable()
    }

    private fun coldHotConnectedable() {
        val observable = Observable.just(1,2,3,4)
        observable.subscribe{item-> Log.i(TAG, "Test Cold $item")}

        Thread.sleep(3000)

        observable.subscribe{item-> Log.i(TAG, "Test Cold 2 $item")}

        Log.i(TAG, "============Hot and connectable================")

        val connectableObserver = Observable.just(1,2,3,4).publish()
        connectableObserver.subscribe{item-> Log.i(TAG, "Test Hot $item")}
        connectableObserver.subscribe{item-> Log.i(TAG, "Test hot 2 $item")}
        connectableObserver.connect()

    }

    private fun utilityOperators() {
        val observable = Observable.just("1", "2", "3", "4","5")
        val observer = object : Observer<Int>{
            override fun onSubscribe(d: Disposable) {
                compositeDisposable.add(d)
                Log.i(TAG, "onSubscribe")
            }

            override fun onNext(t: Int) {
                Log.i(TAG, "onNext : $t")
            }

            override fun onError(e: Throwable) {
                Log.i(TAG, "onError : ${e.localizedMessage}")
            }

            override fun onComplete() {
                Log.i(TAG, "onComplete")
            }
        }
        Log.i(TAG, "============Delay================")
        observable
            .map { it.toInt() }
            .delay(1,TimeUnit.SECONDS)
            .subscribe(observer)

        Log.i(TAG, "============Time Interval================")
        timeInterval()

        Log.i(TAG, "============TimeOut================")
        timeOutDemo()
    }

    private fun timeOutDemo() {
        val observable = Observable.create<String> { emitter ->
            emitter.onNext("A")

            Thread.sleep(2000)
            emitter.onNext("B")

            Thread.sleep(3000)
            emitter.onNext("C")

            Thread.sleep(8000)
            emitter.onNext("D")

            emitter.onComplete()
        }
        val observer = object : Observer<String> {
            override fun onSubscribe(d: Disposable) {
                compositeDisposable.add(d)
                Log.i(TAG, "onSubscribe")
            }

            override fun onNext(t: String) {
                Log.i(TAG, "onNext : $t")
            }

            override fun onError(e: Throwable) {
                Log.i(TAG, "onError : ${e.localizedMessage}")
            }

            override fun onComplete() {
                Log.i(TAG, "onComplete")
            }
        }

        observable
            .timeout(4, TimeUnit.SECONDS)
            .subscribe(observer)
    }

    private fun timeInterval() {
        val observable = Observable.just("1", "2", "3", "4","5")
        val observer = object : Observer<Timed<Int>>{
            override fun onSubscribe(d: Disposable) {
                compositeDisposable.add(d)
                Log.i(TAG, "Time Interval onSubscribe")
            }

            override fun onNext(t: Timed<Int>) {
                Log.i(TAG, "ime Interval onNext : $t")
            }

            override fun onError(e: Throwable) {
                Log.i(TAG, "ime Interval onError : ${e.localizedMessage}")
            }

            override fun onComplete() {
                Log.i(TAG, "ime Interval onComplete")
            }
        }

        observable
            .map { it.toInt() }
            .timeInterval()
            .subscribe(observer)

    }

    private fun errorHandlingOperator() {
        val observable = Observable.just("1", "2", "3", "A","5")
        val observer = object : Observer<Int> {
            override fun onSubscribe(d: Disposable) {
                compositeDisposable.add(d)
                Log.i(TAG, "onSubscribe")
            }

            override fun onNext(t: Int) {
                Log.i(TAG, "onNext : $t")
            }

            override fun onError(e: Throwable) {
                Log.i(TAG, "onError : ${e.localizedMessage}")
            }

            override fun onComplete() {
                Log.i(TAG, "onComplete")
            }
        }
        Log.i(TAG, "============onErrorResumeNext================")
        observable
            .map { it.toInt()}
            .onErrorResumeNext {
                Observable.just(10, 20, 30)
            }
            .subscribe(observer)

        Log.i(TAG, "============onErrorComplete================")
        observable
            .map { it.toInt()}
            .onErrorComplete {
                return@onErrorComplete true
            }
            .subscribe(observer)

        Log.i(TAG, "============Retry================")
        observable
            .map { it.toInt()}
            .retry{count, throwable ->
                return@retry count <=2
            }
            .subscribe(observer)
    }

    private fun combiningObservable() {
        val observable1 = Observable.just("A", "B", "C", "D", "E")
        val observable2 = Observable.just("1", "2", "3", "4")

        val observer = object : Observer<String> {
            override fun onSubscribe(d: Disposable) {
                compositeDisposable.add(d)
                Log.i(TAG, "onSubscribe")
            }

            override fun onNext(t: String) {
                Log.i(TAG, "onNext : $t")
            }

            override fun onError(e: Throwable) {
                Log.i(TAG, "onError : ${e.localizedMessage}")
            }

            override fun onComplete() {
                Log.i(TAG, "onComplete")
            }
        }
        Log.i(TAG, "============Concat================")
        val observableResult = Observable.concat(observable1, observable2)
        observableResult.subscribe(observer)

        Log.i(TAG, "============Marge================")
        val observableMarge = Observable.merge(observable1, observable2)
        observableMarge.subscribe(observer)

        Log.i(TAG, "============Zip================")
        val observableZip = Observable.zip(
            observable1,
            observable2,
            BiFunction<String, String, String> { x, y -> return@BiFunction x + y })
        observableZip.subscribe(observer)
    }


    private fun filterningObservable() {
        val observable = Observable.just(1, 2, 3, 4, 4)

        val observer = object : Observer<Int> {
            override fun onSubscribe(d: Disposable) {
                compositeDisposable.add(d)
                Log.i(TAG, "filtering onSubscribe  :")
            }

            override fun onNext(t: Int) {
                Log.i(TAG, "filtering onNext : $t")
            }

            override fun onError(e: Throwable) {
                Log.i(TAG, "filtering onError  :${e.localizedMessage}")
            }

            override fun onComplete() {
                Log.i(TAG, "filtering onComplete  :")
            }
        }
        Log.i(TAG, "============Skip================")
        observable.skip(2).subscribe(observer)

        Log.i(TAG, "============Distinct================")
//        remove duplicate value
        observable.distinct().subscribe(observer)

        Log.i(TAG, "============Filter================")
        observable
            .filter {
                it in 2..4
            }
            .subscribe(observer)


        Log.i(TAG, "============Debounce================")

        val observableDebounce: Observable<String> = Observable.create { emitter ->
            emitter.onNext("Ball 1")
            Thread.sleep(1500)

            emitter.onNext("Ball 2")
            Thread.sleep(500)

            emitter.onNext("Ball 3")
            Thread.sleep(800)

            emitter.onNext("Ball 4")
            Thread.sleep(2000)

            emitter.onComplete()
        }

        val observerDebounce = object : Observer<String> {
            override fun onSubscribe(d: Disposable) {
                compositeDisposable.add(d)
                Log.i(TAG, "Debounce onSubscribe  :")
            }

            override fun onNext(t: String) {
                Log.i(TAG, "Debounce onNext : $t")
            }

            override fun onError(e: Throwable) {
                Log.i(TAG, "Debounce onError  :${e.localizedMessage}")
            }

            override fun onComplete() {
                Log.i(TAG, "Debounce onComplete  :")
            }
        }
        observableDebounce.debounce(1, TimeUnit.SECONDS).subscribe(observerDebounce)
    }

    private fun OperatorExample() {
        val observable: Observable<String> = Observable.just("Hello", "from happy?")
        val observableInt: Observable<Int> = Observable.just(12, 13, 15)
        val observableScan: Observable<String> = Observable.just("h", "a", "p", "p", "y")

        val observer = object : Observer<String> {
            override fun onSubscribe(d: Disposable) {
                compositeDisposable.add(d)
                Log.i(TAG, "onSubscribe  :")
            }

            override fun onNext(t: String) {
                Log.i(TAG, "onNext Profile name : $t")
            }

            override fun onError(e: Throwable) {
                Log.i(TAG, "onError  :${e.localizedMessage}")
            }

            override fun onComplete() {
                Log.i(TAG, "onComplete  :")
            }
        }
        observable.map { it.uppercase(Locale.ENGLISH) }.subscribe(observer)
        Log.i(TAG, "============FlatMap================")
        observableInt
            .flatMap { id ->
                getProfileName(id)
            }
            .subscribe(observer)
        Log.i(TAG, "==============SCAN====================")
        observableScan
            .scan { x: String?, y: String -> x.plus(y).uppercase() }
            .subscribe(observer)
        Log.i(TAG, "==============Buffer====================")
        bufferOperator()
    }

    private fun bufferOperator() {
        val observable: Observable<String> = Observable.just("h", "a", "p", "p", "y")
        val observer = object : Observer<List<String>> {
            override fun onSubscribe(d: Disposable) {
                compositeDisposable.add(d)
                Log.i(TAG, "onSubscribe  :")
            }

            override fun onNext(list: List<String>) {
                Log.i(TAG, "onNext Profile name : $list")
            }

            override fun onError(e: Throwable) {
                Log.i(TAG, "onError  :${e.localizedMessage}")
            }

            override fun onComplete() {
                Log.i(TAG, "onComplete  :")
            }
        }
        observable
            .buffer(2)
            .subscribe(observer)
    }

    private fun getProfileName(profileId: Int): Observable<String> {
        return when (profileId) {
            12 -> Observable.just("Hello")
            13 -> Observable.just("Chandra")
            else -> Observable.error(Exception("Profile not found"))

        }
    }


    private fun rxJavaOperatorRepeat() {
        val observable = Observable.range(0, 10).repeat(2)

        val observer = object : Observer<Int> {
            override fun onSubscribe(d: Disposable) {
                compositeDisposable.add(d)
                Log.d(TAG, "Range onSubscribe")
            }

            override fun onNext(t: Int) {
                Log.d(TAG, "Range onNext $t")
            }

            override fun onError(e: Throwable) {
                Log.d(TAG, "Range onError ${e.localizedMessage}")
            }

            override fun onComplete() {
                Log.d(TAG, "Range  onComplete")
            }
        }
        Log.i(TAG, "================Range / Repeat Observable============")
        observable.subscribe(observer)
    }

    private fun rxJavaOperatorRange() {
        val observable = Observable.range(0, 10)
        val observableInterval = Observable.interval(1, TimeUnit.SECONDS)
        val observer = object : Observer<Int> {
            override fun onSubscribe(d: Disposable) {
                compositeDisposable.add(d)
                Log.d(TAG, "Interval onSubscribe")
            }

            override fun onNext(t: Int) {
                Log.d(TAG, "Interval onNext $t")
            }

            override fun onError(e: Throwable) {
                Log.d(TAG, "Interval onError ${e.localizedMessage}")
            }

            override fun onComplete() {
                Log.d(TAG, "Interval onComplete")
            }
        }
        Log.i(TAG, "================Range Observable============")
        observable.subscribe(observer)

//        Interval Operator
        val observerInterval = object : Observer<Long> {
            override fun onSubscribe(d: Disposable) {
                compositeDisposable.add(d)
                Log.d(TAG, "Range onSubscribe")
            }

            override fun onNext(t: Long) {
                Log.d(TAG, "Range onNext $t")
            }

            override fun onError(e: Throwable) {
                Log.d(TAG, "Range onError ${e.localizedMessage}")
            }

            override fun onComplete() {
                Log.d(TAG, "Range  onComplete")
            }
        }
        Log.i(TAG, "================Interval Observable============")
        observableInterval.subscribe(observerInterval)
    }

    private fun mayBeObservableRxJava() {
        val maybeObservable = Maybe.just("Hello from androchef")
        val maybeObservableEmpty = Maybe.empty<String>()

        val maybeObserver = object : MaybeObserver<String> {
            override fun onSubscribe(d: Disposable) {
                compositeDisposable.add(d)
                Log.d(TAG, "May Be onSubscribe")
            }

            override fun onSuccess(t: String) {
                Log.d(TAG, "May Be onSuccess : $t")
            }

            override fun onError(e: Throwable) {
                Log.d(TAG, "May Be onError : ${e.localizedMessage}")
            }

            override fun onComplete() {
                Log.d(TAG, "May Be onComplete")
            }

        }
        Log.i(TAG, "================Maybe Observable============")
        maybeObservable
            .map { it.uppercase(Locale.ENGLISH) }
            .subscribe(maybeObserver)

        maybeObservableEmpty
            .map { it.uppercase(Locale.ENGLISH) }
            .subscribe(maybeObserver)
    }

    //    Completable Observable
    private fun completeObservableRxJava() {
        val completableObservable = Completable.complete()
        val completableObservableError = Completable.error(Exception("This is some internal error"))

        val completableObserver = object : CompletableObserver {
            override fun onSubscribe(d: Disposable) {
                compositeDisposable.add(d)
                Log.d(TAG, "Complete Observer onSubscribe")
            }

            override fun onComplete() {
                Log.d(TAG, "Completable Observer onComplete")
            }

            override fun onError(e: Throwable) {
                Log.d(TAG, "Completable Observer onError : ${e.localizedMessage}")
            }
        }
        Log.i(TAG, "================Complete Observable============")
        completableObservable.subscribe(completableObserver)
        completableObservableError.subscribe(completableObserver)
    }

    //    Single Observer
    private fun singleObservableXJava() {
        val singleObservable = Single.just("hello from chandra")

        val singleObserver = object : SingleObserver<String> {
            override fun onSubscribe(d: Disposable) {
                compositeDisposable.add(d)
                Log.d(TAG, "onSubscribe")
            }

            override fun onSuccess(t: String) {
                Log.d(TAG, "OnSuccess : $t")
            }

            override fun onError(e: Throwable) {
                Log.d(TAG, "onError ${e.localizedMessage}")
            }

        }
        Log.i(TAG, "================Single Observable============")
        singleObservable
            .map { it.uppercase(Locale.ENGLISH) }
            .subscribe(singleObserver)
        Log.i(TAG, "================ Single Observable count operator ============")
        singObservableCount()
    }

    private fun singObservableCount() {
        val singleObservable = Observable.just(1,2,3,4,5,6)

        val singleObserver = object : SingleObserver<Long> {
            override fun onSubscribe(d: Disposable) {
                compositeDisposable.add(d)
                Log.d(TAG, "onSubscribe")
            }

            override fun onSuccess(t: Long) {
                Log.d(TAG, "OnSuccess  Count : $t")
            }

            override fun onError(e: Throwable) {
                Log.d(TAG, "onError ${e.localizedMessage}")
            }
        }
        singleObservable
            .count()
            .subscribe(singleObserver)
    }


    private fun showDataFlowRXJava() {

//        creation of observable
        val observable = Observable.just("hello", "chandra")

//        Consumer
        val observer = object : Observer<String> {
            override fun onSubscribe(d: Disposable) {
                compositeDisposable.add(d)
                Log.e(TAG, "onSubscribe")
            }

            override fun onNext(t: String) {
                Log.e(TAG, "onNext : $t")
            }

            override fun onError(e: Throwable) {
                Log.e(TAG, "onError :  ${e.localizedMessage}")
            }

            override fun onComplete() {
                Log.e(TAG, "onCompleted")
            }
        }

//      Transformation
        observable.map { it.uppercase(Locale.ENGLISH) }
            .subscribe(observer) // subscription // execution
    }

    //    Destruction
    override fun onDestroy() {
        super.onDestroy()
        if(compositeDisposable.isDisposed.not()){
            compositeDisposable.dispose()
        }
    }

    companion object {
        const val TAG = "MY_APP"
    }

}